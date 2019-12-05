'''
ipython compatability module for nvd3-python
This adds simple ipython compatibility to the nvd3-python package, without making any
major modifications to how the main package is structured.  It utilizes the IPython
display-formatter functionality, as described at:
http://nbviewer.ipython.org/github/ipython/ipython/blob/master/examples/notebooks/Custom%20Display%20Logic.ipynb
For additional examples, see:
https://github.com/sympy/sympy/blob/master/sympy/interactive/printing.py
'''

try:
    _ip = get_ipython()
except:
    _ip = None
if _ip and _ip.__module__.lower().startswith('ipy'):
    global _js_initialized
    _js_initialized = False

    def _print_html(chart):
        '''Function to return the HTML code for the div container plus the javascript
        to generate the chart.  This function is bound to the ipython formatter so that
        charts are displayed inline.'''
        global _js_initialized
        if not _js_initialized:
            print('js not initialized - pausing to allow time for it to load...')
            initialize_javascript()
            import time
            time.sleep(5)
        chart.buildhtml()
        return chart.htmlcontent

    def _setup_ipython_formatter(ip):
        ''' Set up the ipython formatter to display HTML formatted output inline'''
        from IPython import __version__ as IPython_version
        from nvd3 import __all__ as nvd3_all

        if IPython_version >= '0.11':
            html_formatter = ip.display_formatter.formatters['text/html']
            for chart_type in nvd3_all:
                html_formatter.for_type_by_name('nvd3.' + chart_type, chart_type, _print_html)

    def initialize_javascript(d3_js_url='https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.5/d3.min.js',
                              nvd3_js_url='https://cdnjs.cloudflare.com/ajax/libs/nvd3/1.7.0/nv.d3.min.js',
                              nvd3_css_url='https://cdnjs.cloudflare.com/ajax/libs/nvd3/1.7.0/nv.d3.min.css',
                              use_remote=False):
        '''Initialize the ipython notebook to be able to display nvd3 results.
        by instructing IPython to load the nvd3 JS and css files, and the d3 JS file.

        by default, it looks for the files in your IPython Notebook working directory.

        Takes the following options:

        use_remote: use remote hosts for d3.js, nvd3.js, and nv.d3.css (default False)
        * Note:  the following options are ignored if use_remote is False:
        nvd3_css_url: location of nvd3 css file (default https://cdnjs.cloudflare.com/ajax/libs/nvd3/1.7.0/nv.d3.min.css)
        nvd3_js_url: location of nvd3 javascript file (default  https://cdnjs.cloudflare.com/ajax/libs/nvd3/1.7.0/nv.d3.min.css)
        d3_js_url: location of d3 javascript file (default https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.5/d3.min.js)
        '''
        from IPython.display import display, Javascript, HTML

        if not use_remote:
            # these file locations are for IPython 1.x, and will probably change when 2.x is released
            d3_js_url = 'files/d3.v3.js'
            nvd3_js_url = 'files/nv.d3.js'
            nvd3_css_url = 'files/nv.d3.css'

        # load the required javascript files

        #display(Javascript('''$.getScript("%s")''' %(d3_js_url)))
        display(HTML('''<link media="all" href="%s" type="text/css"
                        rel="stylesheet"/>''' % (nvd3_css_url)))
        # The following two methods for loading the script file are redundant.
        # This is intentional.
        # Ipython's loading of javscript in version 1.x is a bit squirrely, especially
        # when creating demos to view in nbviewer.
        # by trying twice, in two different ways (one using jquery and one using plain old
        # HTML), we maximize our chances of successfully loading the script.
        display(Javascript('''$.getScript("%s")''' % (nvd3_js_url)))
        display(Javascript('''$.getScript("%s", function() {
                              $.getScript("%s", function() {})});''' % (d3_js_url, nvd3_js_url)))
        display(HTML('<script src="%s"></script>' % (d3_js_url)))
        display(HTML('<script src="%s"></script>' % (nvd3_js_url)))

        global _js_initialized
        _js_initialized = True

    print('loaded nvd3 IPython extension\n'
          'run nvd3.ipynb.initialize_javascript() to set up the notebook\n'
          'help(nvd3.ipynb.initialize_javascript) for options')

    _setup_ipython_formatter(_ip)
