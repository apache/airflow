import json
from typing import Any, List
from plugins.models.base import Base
from sqlalchemy import Column, Integer, String, Text, Boolean
from airflow.utils.db import provide_session
from airflow.plugins_manager import AirflowPlugin
from airflow import settings


class CurveTemplateModel(Base):
    """
    curve template.
    """
    __tablename__ = "curve_template"
    __NO_DEFAULT_SENTINEL = object()

    id = Column(Integer, primary_key=True)
    key = Column(String(250), unique=True)
    val = Column('val', Text)
    active = Column(Boolean, default=True)

    def __repr__(self):
        return '{} : {}'.format(self.key, self.val)

    def __init__(self, *args, key=None, val=None, active=True, **kwargs):
        super(CurveTemplateModel, self).__init__(*args, **kwargs)
        self.key = key
        self.val = val
        self.active = active

    @classmethod
    def get(
        cls,
        key,  # type: str
        default_var=__NO_DEFAULT_SENTINEL,  # type: Any
        deserialize_json=False  # type: bool
    ):
        from airflow.models.variable import Variable
        var_val = Variable.get_variable_from_secrets(key=key)
        if var_val is None:
            if default_var is not cls.__NO_DEFAULT_SENTINEL:
                return default_var
            else:
                raise KeyError('Curve Template {} does not exist'.format(key))
        else:
            if deserialize_json:
                return json.loads(var_val)
            else:
                return var_val

    @classmethod
    @provide_session
    def set(
        cls,
        key,  # type: str
        value,  # type: Any
        serialize_json=False,  # type: bool
        session=None
    ):
        if serialize_json:
            stored_value = json.dumps(value, indent=2, separators=(',', ': '), ensure_ascii=False)
        else:
            stored_value = str(value)

        cls.delete(key, session=session)
        session.add(CurveTemplateModel(key=key, val=stored_value))  # type: ignore
        session.flush()

    @classmethod
    @provide_session
    def delete(cls, key, session=None):
        session.query(cls).filter(cls.key == key).delete()

    @classmethod
    @provide_session
    def update(
        cls,
        key,  # type: str
        value,  # type: Any
        serialize_json=False,  # type: bool
        session=None
    ):

        if serialize_json:
            stored_value = json.dumps(value, indent=2, separators=(',', ': '))
        else:
            stored_value = str(value)
        session.query(cls).filter(cls.key == key).update({cls.val: stored_value})  # type: ignore
        session.flush()

    @classmethod
    @provide_session
    def get_all_active_curve_tmpls(cls, session=None):
        tmpls: List[cls] = session.query(cls).filter(cls.active).all()
        ret = {}
        for tmpl in tmpls:
            ret[tmpl.key] = tmpl.val
        return ret

    @classmethod
    @provide_session
    def get_fuzzy_active(
        cls,
        key,  # type: str
        deserialize_json=False,  # type: bool
        session=None,
        default_var=__NO_DEFAULT_SENTINEL
    ):
        key_p = "%{}%".format(key)
        obj = session.query(cls).filter(cls.key.like(key_p), cls.active).first()
        if obj is None:
            if default_var is not cls.__NO_DEFAULT_SENTINEL:
                return key, default_var
            raise KeyError('Curve Template {} does not exist'.format(key))
        if deserialize_json:
            return obj.key, json.loads(obj.val)
        return obj.key, obj.val


# Defining the plugin class
class CurveTemplateModelPlugin(AirflowPlugin):
    name = "curve_template_model_plugin"

    @classmethod
    def on_load(cls):
        engine = settings.engine
        if not engine.dialect.has_table(engine, CurveTemplateModel.__tablename__):
            Base.metadata.create_all(engine)
