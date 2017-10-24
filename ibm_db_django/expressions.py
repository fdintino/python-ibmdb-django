from django.db.models.expressions import Expression, RawSQL, F
from django.db.models.lookups import BuiltinLookup
from django.utils.encoding import python_2_unicode_compatible
from django.utils import six

from .fields import XMLField


def quote_xml_name(name):
    if name.startswith("\"") and name.endswith("\""):
        return name
    elif name.startswith("\""):
        return "%s\"" % name
    elif name.endswith("\""):
        return "\"%s" % name
    else:
        return "\"%s\"" % name


def str_literal(val):
    return "'%s'" % val.replace("'", "''")


class VeryRawSQL(RawSQL):
    """Same as RawSQL, but not wrapped in parentheses"""

    def as_sql(self, compiler, connection):
        return '%s' % self.sql, self.params


class SummarizedF(F):

    def resolve_expression(self, query=None, allow_joins=True, reuse=None,
                           summarize=True, for_save=False):
        return super(SummarizedF, self).resolve_expression(
            query, allow_joins, reuse, summarize, for_save)


class FXML(F):

    def resolve_expression(self, *args, **kwargs):
        return self

    def as_sql(self, compiler, connection):
        return quote_xml_name(self.name), []

    @property
    def contains_aggregate(self):
        return False


@python_2_unicode_compatible
class XQuery(Expression):
    template = 'XMLQUERY(%(query)s PASSING %(field_name)s as %(alias)s)'

    def __init__(self, field, query, alias="doc", output_field=None):
        if isinstance(field, six.string_types):
            field = SummarizedF(field)
        self.field_name = field
        if isinstance(query, six.string_types):
            query = VeryRawSQL(str_literal(query), [])
        self.query = query
        if isinstance(alias, six.string_types):
            alias = FXML(alias)
        self.alias = alias
        output_field = output_field or XMLField()
        super(XQuery, self).__init__(output_field=output_field)

    def __str__(self):
        return 'XMLQUERY(%r PASSING %r as %r)' % (self.query, self.field_name, self.alias)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self)

    def get_source_expressions(self):
        return [self.field_name, self.query, self.alias]

    def set_source_expressions(self, exprs):
        self.field_name, self.query, self.alias = exprs

    def get_source_fields(self):
        return []

    def compile_without_params(self, value, compiler):
        sql, params = compiler.compile(value)
        for param in params:
            sql = sql.replace(str_literal(param), '?', 1)
        return sql

    def as_sql(self, compiler, connection):
        connection.ops.check_expression_support(self)
        params = {
            'query': self.compile_without_params(self.query, compiler),
            'alias': self.compile_without_params(self.alias, compiler),
            'field_name': self.compile_without_params(self.field_name, compiler),
        }
        template = self.template
        if not isinstance(self.output_field, XMLField):
            template = "XMLCAST(%s AS %%(db_type)s)" % template
            params['db_type'] = self.output_field.db_type(connection)
        return template % params, []

    def convert_value(self, value, expression, connection, context):
        if self.output_field.get_internal_type() == "XMLField":
            return self.output_field.to_python(value)
        return super(XQuery, self).convert_value(value, expression, connection, context)


class XMLExists(BuiltinLookup):
    lookup_name = 'xmlexists'

    def as_sql(self, compiler, connection):
        lhs, lhs_params = compiler.compile(self.lhs)
        rhs = "'%s'" % self.rhs.replace("'", "''")
        return ('XMLEXISTS(%s PASSING %s AS "doc")' % (rhs, lhs), [])
