from .dags import dags_blueprint

def init_app(app):
    app.register_blueprint(dags_blueprint)