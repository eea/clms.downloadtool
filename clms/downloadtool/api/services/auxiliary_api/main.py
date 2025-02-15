"""auxiliary api"""

# -*- coding: utf-8 -*-
import json
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from ftplib import FTP

import requests


def get_landcover(api_url, dataset_path, x_max, y_max, x_min, y_min):
    """get data for land cover"""
    files_to_download = []

    if x_max == "" or y_max == "" or x_min == "" or y_min == "":
        x_max = "32.871094"
        y_max = "70.289117"
        x_min = "-12.480469"
        y_min = "35.603719"

    x_min_aux = float(x_min)
    x_max_aux = float(x_max)

    while x_min_aux < x_max_aux:
        longitude = ""
        if x_min_aux < 0:
            longitude = "W"
        else:
            longitude = "E"

        longitude += (
            (str(abs(int(x_min_aux - (x_min_aux % 20))))).ljust(2, "0")
        ).rjust(3, "0")

        x_min_aux += 20

        y_min_aux = float(y_min)
        y_max_aux = float(y_max)

        while (20 + y_min_aux) - (y_min_aux % 20) <= (20 + y_max_aux) - (
            y_max_aux % 20
        ):
            latitude = ""
            if y_min_aux < 0:
                latitude = "S"
            else:
                latitude = "N"

            latitude += (str(abs(int(y_min_aux - (y_min_aux % 20) + 20)))
                         ).ljust(2, "0")

            y_min_aux += 20

            running = True
            token = ""
            while running:
                # pylint: disable=line-too-long
                url = "{}?list-type=2&max-keys=1000&prefix={}/{}{}".format(  # noqa
                    api_url, dataset_path, longitude, latitude
                )
                if token != "":
                    url += "&continuation-token=" + urllib.parse.quote(token)

                response = requests.get(url)

                tree = ET.fromstring(response.text)
                namespace = get_namespace(tree.tag)

                for key in tree.iter(namespace + "Key"):
                    files_to_download.append(api_url + "/" + key.text)

                if tree.find(namespace + "IsTruncated").text == "true":
                    token = tree.find(namespace + "NextContinuationToken").text
                else:
                    running = False

    return files_to_download


def get_namespace(tag):
    """get the namespace"""
    m = re.match(r"\{.*\}", tag)
    return m.group(0) if m else ""


def get_wekeo(
    api_url,
    username,
    password,
    dataset_path,
    wekeo_choices,
    date_from,
    date_to,
    x_max,
    y_max,
    x_min,
    y_min,
):
    """get data from wekeo"""
    if date_from == "" or date_to == "":
        metadata_text = get_wekeo_metadata(
            api_url, username, password, dataset_path)
        metadata = json.loads(metadata_text)
        date_from = metadata['properties']['startdate']['default']
        date_to = metadata['properties']['enddate']['default']

    if date_from == "" or date_to == "":
        date_to = datetime.now().strftime("%Y-%m-%d")
        date_from = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        date_from += "T00:00:00.000Z"
        date_to += "T00:00:00.000Z"

    if x_max == "" or y_max == "" or x_min == "" or y_min == "":
        x_max = "32.871094"
        y_max = "70.289117"
        x_min = "-12.480469"
        y_min = "35.603719"

    return json.loads(
        wekeo_choices.replace("@xmin", x_min)
        .replace("@ymin", y_min)
        .replace("@xmax", x_max)
        .replace("@ymax", y_max)
        .replace("@startdate", date_from)
        .replace("@enddate", date_to)
    )


def get_wekeo_token(api_url, username, password):
    """get wekeo token"""
    data = {"username": username, "password": password}
    response = requests.post(api_url + "gettoken", json=data, verify=True)
    result = json.loads(response.text)
    return result["access_token"]


def get_wekeo_metadata(api_url, username, password, dataset_path):
    """get wekeo metadata"""
    token = get_wekeo_token(api_url, username, password)
    my_headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(
        api_url + "api/v1/dataaccess/queryable/" + dataset_path,
        headers=my_headers
    )
    result = response.text
    return result


def get_legacy(username, password, path, date_from, date_to):
    """get legacy data"""
    files_to_download = []

    # pylint: disable=too-many-nested-blocks
    if path.startswith("ftp"):
        ftp_params = path.replace("ftp://", "").split("/")
        ftp = FTP(ftp_params[0], username, password)
        ftp_path = ""
        for index, ftp_param in enumerate(ftp_params):
            if index != 0 and ftp_param != "":
                if ftp_path == "":
                    ftp_path = ftp_param
                else:
                    ftp_path += "/" + ftp_param

        files = ftp.nlst(ftp_path)

        for file in files:
            if file.endswith(".nc"):
                if date_from != "" and date_to != "":
                    date_file_aux = extract_date_legacy_ftp(file)
                    date_from_datetime = datetime.strptime(
                        date_from, "%Y-%m-%d")
                    date_file_aux_datetime = datetime.strptime(
                        date_file_aux, "%Y%m%d%H%M"
                    )
                    date_to_datetime = datetime.strptime(date_to, "%Y-%m-%d")

                    # pylint: disable=line-too-long
                    if date_from_datetime <= date_file_aux_datetime <= date_to_datetime:  # noqa
                        files_to_download.append(path + file.split("/")[-1])
                else:
                    if len(files_to_download) == 0:
                        files_to_download.append(path + file.split("/")[-1])
                    else:
                        date_file = extract_date_legacy_ftp(
                            files_to_download[0])
                        date_file_aux = extract_date_legacy_ftp(file)

                        if date_file != "" and date_file_aux != "":
                            # pylint: disable=line-too-long
                            if datetime.strptime(
                                date_file, "%Y%m%d%H%M"
                            ) < datetime.strptime(date_file_aux, "%Y%m%d%H%M"):  # noqa
                                files_to_download[0] = path + \
                                    file.split("/")[-1]
    else:
        # pylint: disable=consider-using-with
        data = urllib.request.urlopen(path)

        for line in data:
            file = line.decode("utf-8").strip("\n")
            if file != "":
                if date_from != "" and date_to != "":
                    date_file_aux = extract_date_legacy_http(file)
                    date_from_datetime = datetime.strptime(
                        date_from, "%Y-%m-%d")
                    date_to_datetime = datetime.strptime(date_to, "%Y-%m-%d")
                    # pylint: disable=line-too-long
                    if date_from_datetime <= date_file_aux <= date_to_datetime:  # noqa
                        files_to_download.append(file)
                else:
                    if len(files_to_download) == 0:
                        files_to_download.append(file)
                    else:
                        date_file = extract_date_legacy_http(
                            files_to_download[0])
                        date_file_aux = extract_date_legacy_http(file)

                        if date_file != "" and date_file_aux != "":
                            if date_file < date_file_aux:
                                files_to_download[0] = file

    return files_to_download


def extract_date_legacy_http(text):
    """extract date from legacy http"""

    result_date = ""
    result_filename = text.split("/")[-1]
    # validate that the file name is not empty
    if len(result_filename) > 0:
        # Gets the date that is in the manifest name
        rdates = result_filename.split("_")[3]

        if len(rdates) == 14:
            # Case: YYYY1-YYYY2-MMDD
            # zz: find a case for this in order to test. It seems wrong.
            f_startDate = rdates  # not sure
            f_endDate = rdates  # not sure
            startDate = f_startDate[0:4]
            endDate = f_endDate[0:4]
            # date1 = rdates[0:4]
            # date2 = rdates[5:9]

            month_day_result = rdates[len(rdates) - 4: len(rdates)]
            if startDate == endDate:
                result_date = startDate + month_day_result
            elif startDate < endDate:
                result_date = endDate + month_day_result
        else:
            rsearch = str("----")
            initPos = rdates.find(rsearch)
            if initPos > 0:
                # Case: YYYY1----YYYY2
                result_date = rdates[0:4] + "0101"
            else:
                # Case: YYYYMMDDhhmm
                result_date = rdates[0:8]

    date_aux = datetime.strptime(result_date, "%Y%m%d")

    return date_aux


def extract_date_legacy_ftp(text):
    """extract date from legacy ftp"""
    date_text = ""
    date_aux = re.findall(r"\d{12}", text)
    if len(date_aux) > 0:
        date_text = date_aux[0]

    return date_text


if __name__ == "__main__":
    get_landcover(
        "https://s3-eu-west-1.amazonaws.com/vito.landcover.global",
        "v3.0.1/2016",
        "",
        "",
        "",
        "",
    )
