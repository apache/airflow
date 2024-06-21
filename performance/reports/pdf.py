import logging
import os

import markdown
import pdfkit
from bs4 import BeautifulSoup

HTML = """\
<body>
<head>
<style>
    p, td, h1, h2, h3, li {{font-family: arial;}}
    table, td, th {{border: 1px solid black;}}
    table {{width: 80%; border-collapse: collapse}}
    td {{width:0.1%;; text-align: left; white-space:nowrap;}}
    img {{width: 100%;}}
</style>
</head>
{}
</body>
"""

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def generate_pdf(path_to_md_report: str) -> None:
    """
    Converts an markdown report to pdf version. The pdf report
    is saved under the same name as original one.

    :param path_to_md_report: path to Markdown report
    """
    log.info("Generating pdf report from %s", path_to_md_report)
    md_report_directory = os.path.dirname(path_to_md_report)
    mrkd = markdown.Markdown(extensions=["markdown.extensions.tables"])
    with open(path_to_md_report, "r") as f:
        md_content = f.read()
    raw_html = mrkd.convert(md_content)

    soup = BeautifulSoup(HTML.format(raw_html), "html.parser")

    # Customize images
    for img in soup.find_all("img"):
        img.attrs["src"] = os.path.join(md_report_directory, img.get("src"))

    options = {
        "enable-local-file-access": None,
        "page-size": "A4",
        "orientation": "landscape",
        "encoding": "UTF-8",
    }

    pdf_file_name = os.path.basename(path_to_md_report).split(".")[0] + ".pdf"
    pdf_file_path = os.path.join(md_report_directory, pdf_file_name)
    try:
        pdfkit.from_string(soup.prettify(), pdf_file_path, options=options)
        log.info("Report saved under %s", pdf_file_path)
    except IOError as err:
        # Possibly the wkhtmltopdf executable is not installed
        log.exception(str(err))
