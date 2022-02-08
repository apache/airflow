# ~~~~~~~~~~~~~~~~~~~~~~
#
# Server-side Sessions and SessionInterfaces.
#
# Originally copied from flask_session.sessions
# Flask-Session
# Copyright (C) 2014 by Shipeng Feng and other contributors
# BSD, see LICENSE for more details.

from datetime import datetime
import json
from uuid import uuid4

from flask.sessions import SessionInterface as FlaskSessionInterface
from flask.sessions import SessionMixin as FlaskSessionMixin
from werkzeug.datastructures import CallbackDict
from itsdangerous import Signer, BadSignature, want_bytes

from airflow.utils.session import create_session

from .db_models import Session as SessionModel


class SQLAlchemySession(CallbackDict, FlaskSessionMixin):
    """Baseclass for server-side based sessions."""

    def __init__(self, initial=None, sid=None, permanent=None):
        def on_update(self):
            self.modified = True
        super().__init__(initial, on_update)
        self.sid = sid
        if permanent:
            self.permanent = permanent
        self.modified = False


class SessionInterface(FlaskSessionInterface):
    def _generate_sid(self):
        return str(uuid4())

    def _get_signer(self, app):
        if not app.secret_key:
            return None
        # The salt is combined with app.secret_key, and is used to "distinguish
        # signatures in different contexts"
        return Signer(app.secret_key, salt='airflow',
                      key_derivation='hmac')


class SQLAlchemySessionInterface(SessionInterface):
    """Uses the Flask-SQLAlchemy from a flask app as a session backend.

    .. versionadded:: 0.2

    :param db: A Flask-SQLAlchemy instance.
    :param table: The table name you want to use.
    :param key_prefix: A prefix that is added to all store keys.
    :param use_signer: Whether to sign the session id cookie or not.
    :param permanent: Whether to use permanent session or not.
    """

    serializer = json
    session_class = SQLAlchemySession
    sql_session_model = SessionModel

    def __init__(self, app, db, table, key_prefix, use_signer=False,
                 permanent=True):
        if db is None:
            from flask_sqlalchemy import SQLAlchemy
            db = SQLAlchemy(app)
        self.db = db
        self.key_prefix = key_prefix
        self.use_signer = use_signer
        self.permanent = permanent
        self.has_same_site_capability = hasattr(self, "get_cookie_samesite")

        self.sql_session_model = SessionModel

    def open_session(self, app, request):
        sid = request.cookies.get(app.session_cookie_name)
        if not sid:
            sid = self._generate_sid()
            return self.session_class(sid=sid, permanent=self.permanent)
        if self.use_signer:
            signer = self._get_signer(app)
            if signer is None:
                return None
            try:
                sid_as_bytes = signer.unsign(sid)
                sid = sid_as_bytes.decode()
            except BadSignature:
                sid = self._generate_sid()
                return self.session_class(sid=sid, permanent=self.permanent)

        store_id = self.key_prefix + sid
        with create_session() as db_session:
            saved_session = db_session.query(self.sql_session_model).filter_by(
                session_id=store_id).first()
            if saved_session and saved_session.expiry <= datetime.utcnow():
                # Delete expired session
                db_session.delete(saved_session)
                saved_session = None
            if saved_session:
                try:
                    val = saved_session.data
                    data = self.serializer.loads(val)
                    return self.session_class(data, sid=sid)
                except:
                    return self.session_class(sid=sid, permanent=self.permanent)
            return self.session_class(sid=sid, permanent=self.permanent)

    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)
        path = self.get_cookie_path(app)
        store_id = self.key_prefix + session.sid
        db_session = self.db.session()
        with create_session() as db_session:
            saved_session = db_session.query(self.sql_session_model).filter_by(
                session_id=store_id).first()

            # The session will evalutate to false once it has been modified to be empty
            if not session:
                # If the session is modified to be empty, remove the cookie.
                if session.modified:
                    if saved_session:
                        db_session.delete(saved_session)
                    response.delete_cookie(app.session_cookie_name,
                                           domain=domain, path=path)
                # If the session is empty, return without setting the cookie.
                return

            conditional_cookie_kwargs = {}
            httponly = self.get_cookie_httponly(app)
            secure = self.get_cookie_secure(app)
            if self.has_same_site_capability:
                conditional_cookie_kwargs["samesite"] = self.get_cookie_samesite(app)
            expires = self.get_expiration_time(app, session)
            val = self.serializer.dumps(dict(session))
            if saved_session:
                saved_session.data = val
                saved_session.expiry = expires
            else:
                new_session = self.sql_session_model(store_id, val, expires)
                db_session.add(new_session)
            if self.use_signer:
                session_id = self._get_signer(app).sign(want_bytes(session.sid))
            else:
                session_id = session.sid
            response.set_cookie(app.session_cookie_name, session_id,
                                expires=expires, httponly=httponly,
                                domain=domain, path=path, secure=secure,
                                **conditional_cookie_kwargs)
