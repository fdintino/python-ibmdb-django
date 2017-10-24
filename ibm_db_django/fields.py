from django.db import models
from django.utils.translation import ugettext_lazy as _
from django.utils import six


class XMLField(models.TextField):
    description = _("XML text")

    def __init__(self, verbose_name=None, name=None, schema_path=None, **kwargs):
        from ibm_db_django.expressions import XMLExists

        self.schema_path = schema_path
        models.Field.__init__(self, verbose_name, name, **kwargs)
        self.class_lookups['xmlexists'] = XMLExists

    def get_internal_type(self):
        return "XMLField"

    def to_python(self, value):
        if value and isinstance(value, six.text_type):
            if value[0] == u'\ufeff':
                value = value[1:]
            xml_decl_utf16 = '<?xml version="1.0" encoding="UTF-16" ?>'
            xml_decl_utf8 = '<?xml version="1.0" encoding="UTF-8" ?>'
            if value[:len(xml_decl_utf16)] == xml_decl_utf16:
                value = "%s%s" % (xml_decl_utf8, value[len(xml_decl_utf16):])
        return value
