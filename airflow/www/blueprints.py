from flask import (
    url_for, Markup, Blueprint, redirect,
)
import chartkick
import markdown

# Init for chartkick, the python wrapper for highcharts
ck = Blueprint(
    'ck_page', __name__,
    static_folder=chartkick.js(), static_url_path='/static')

routes = Blueprint('routes', __name__)


@routes.route('/')
def index():
    return redirect(url_for('admin.index'))


@routes.route('/health')
def health():
    """ We can add an array of tests here to check the server's health """
    content = Markup(markdown.markdown("The server is healthy!"))
    return content
