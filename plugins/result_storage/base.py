"""
此插件的model继承此Base类，创建表时只创建此插件的表
"""
from sqlalchemy.ext.declarative import as_declarative


@as_declarative(name="Base")
class Base(object):
    __table_args__ = {"extend_existing": True}
