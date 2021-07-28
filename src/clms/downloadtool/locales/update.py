# -*- coding: utf-8 -*-
"""Mangage translation strings
"""
import os
import subprocess
import pkg_resources


domain = "clms.downloadtool"
os.chdir(pkg_resources.resource_filename(domain, ""))
os.chdir("../../../")
target_path = "src/clms/downloadtool/"
locale_path = target_path + "locales/"
i18ndude = "./bin/i18ndude"

# ignore node_modules files resulting in errors
excludes = '"*.html *json-schema*.xml"'


def locale_folder_setup():
    """Setup locale folder"""
    os.chdir(locale_path)
    languages = [d for d in os.listdir(".") if os.path.isdir(d)]
    for lang in languages:
        folder = os.listdir(lang)
        if "LC_MESSAGES" not in folder:
            lc_messages_path = lang + "/LC_MESSAGES/"
            os.mkdir(lc_messages_path)
            # pylint: disable=line-too-long
            cmd = "msginit --locale={lang} --input={domain}.pot --output={lang}/LC_MESSAGES/{domain}.po".format(  # NOQA: E501
                lang=lang, domain=domain
            )
            subprocess.call(
                cmd,
                shell=True,
            )

    os.chdir("../../../../")


def _rebuild():
    """Rebuild locale files from po files"""
    # pylint: disable=line-too-long
    cmd = "{i18ndude} rebuild-pot --pot {locale_path}/{domain}.pot --exclude {excludes} --create {domain} {target_path}".format(  # NOQA: E501
        i18ndude=i18ndude,
        locale_path=locale_path,
        domain=domain,
        target_path=target_path,
        excludes=excludes,
    )
    subprocess.call(
        cmd,
        shell=True,
    )


def _sync():
    """Sync translation files"""
    # pylint: disable=line-too-long
    cmd = "{0} sync --pot {locale_path}/{domain}.pot {locale_path}*/LC_MESSAGES/{domain}.po".format(  # NOQA: E501
        i18ndude,
        locale_path=locale_path,
        domain=domain
    )
    subprocess.call(
        cmd,
        shell=True,
    )


def update_locale():
    """Main entry point to update locales"""
    locale_folder_setup()
    _sync()
    _rebuild()
