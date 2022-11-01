import sys
import requests
import os
import numbers
import math
import bisect
import numpy as np
import random
import time
import pandas as pd

from numpy import NAN
from tqdm import tqdm
from bs4 import BeautifulSoup
from requests import Response
from urllib.parse import urlparse, ParseResult
from nhanesVariables import tests


def get_url_base(url: str) -> str:
    parsed_uri: ParseResult = urlparse(url)
    result: str = '{uri.scheme}://{uri.netloc}'.format(uri=parsed_uri)
    return result


def augment_url_with_site(url: str, site: str, pref: str = "http") -> str:
    if pref not in url:
        url = get_url_base(site) + ("/" if url[0] != "/" else "") + url
    return url


def get_links(url: str, extensions: [str]) -> []:
    r: Response = requests.get(url)
    contents: bytes = r.content

    soup = BeautifulSoup(contents, "lxml")
    links: [] = []
    for link in soup.findAll('a'):
        try:
            for extension in extensions:
                if extension in link['href']:
                    links.append(link['href'])
        except KeyError:
            pass
    return links


def remove_prefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


def list_links(url: str, extension: [] = [""]) -> None:
    for x in get_links(url, extension):
        print(x)


# maybe link is str and should be removeprefix
def go_through_directory(path_removal: str, link, output_dir: str) -> None:
    link = link.remove_prefix(path_removal)
    # while "\\" in link:
    # os.makedirs(path, exist_ok=True)


def download_links(links: [], path_removal: str, output_dir: str) -> None:
    cpt = 1
    for link in links:
        link2 = remove_prefix(link, path_removal)
        cur_dir = os.path.dirname(link2)
        new_dir = output_dir + "\\" + cur_dir
        fname = new_dir + "\\" + os.path.basename(link2)
        print(link)
        print("file ", cpt, " / ", len(links), " (" + fname + ")")
        cpt += 1
        try:
            os.makedirs(new_dir, exist_ok=True)
            if not os.path.isfile(fname):
                response = requests.get(link, stream=True)
                with open(fname, "wb") as handle:
                    for data in tqdm(response.iter_content()):
                        handle.write(data)
                    handle.close()
            else:
                print("Skipped file as already created")
        except:
            print("!!! PROBLEM Creating ", fname)


def download_url_links(url: str, extensions: [], path_removal: str, output_dir: str) -> None:
    links: [] = get_links(url, extensions)
    links = [augment_url_with_site(x, url) for x in links]
    download_links(links, path_removal, output_dir)


def download_nhanes(comp: [], years: [], url_type: int = 1) -> None:
    #  prefix="https://wwwn.cdc.gov"
    for y in years:
        for c in comp:
            print()
            print(
                "======================================================================================================================")
            if url_type == 1:
                url = "https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=" + c + "&CycleBeginYear=" + y
                removal = "https://wwwn.cdc.gov/Nchs/"
            else:
                url = "https://wwwn.cdc.gov/nchs/nhanes/ContinuousNhanes/" + c + ".aspx?BeginYear=" + y
                removal = "https://wwwn.cdc.gov/nchs/data/"
            print(y, ":", c, "       =>", url)
            print("")
            types = [".XPT", ".dat", ".sas", ".txt", ".pdf", ".doc"];
            links = get_links(url, [".XPT", ".dat", ".sas", ".txt", ".pdf", ".doc"])
            links_htm = get_links(url, [".htm"])
            for link in links_htm:
                pre, ext = os.path.splitext(link)
                link_xpt = pre + ".XPT"
                link_dat = pre + ".dat"
                link_sas = pre + ".sas"
                if (link_xpt in links) or (link_dat in links) or (link_sas in links):
                    links.append(link)
            # links = [prefix + sub for sub in links]
            links = [augment_url_with_site(x, url) for x in links]
            random.shuffle(links)
            download_links(links, removal, "C:\Tmp\\")


def download_nhanes_b(comp: [], years: []) -> None:
    for year in years:
        for c in comp:
            download_links("https://wwwn.cdc.gov/nchs/nhanes/ContinuousNhanes/" + c + ".aspx?BeginYear=" + year,
                           [".XPT", ".dat", ".sas", ".txt", ".pdf", ".doc"], "https://wwwn.cdc.gov/Nchs/data/",
                           "C:\Tmp\\")


def download_all_nhanes() -> None:
    # downloadNhanes(["Demographics"],["2017"]);
    # listLinks("https://wwwn.cdc.gov/nchs/nhanes/Search/DataPage.aspx?Component=Demographics&CycleBeginYear=2017")

    download_nhanes(["Demographics", "Dietary", "Examination", "Laboratory", "Questionnaire", "Non-Public"],
                    ["2017", "2015", "2013", "2011", "2009", "2007", "2005", "2003", "2001", "1999"])
    download_nhanes(["Questionnaires", "labmethods", "Manuals", "Documents", "overview", "releasenotes", "overviewlab",
                     "overviewquex", "overviewexam"],
                    ["2017", "2015", "2013", "2011", "2009", "2007", "2005", "2003", "2001", "1999"], url_type=2)

    # downloadNhanes(["Demographics","Dietary","Examination","Questionnaire","Non-Public"],["1999"]);
    # downloadNhanesB(["Questionnaires","LabMethods","Manuals","Documents","DocContents","OverviewLab","OverviewQuex","OverviewExam"],["1999"]);
    # downloadLinks("https://wwwn.cdc.gov/nchs/nhanes/nhanes3/DataFiles.aspx", [".xpt",".dat",".sas",".txt",".pdf",".doc"], "https://wwwn.cdc.gov/nchs/data", "E:\Ben\Research\Datasets\Life Science\\")
    # downloadLinks("https://www.cdc.gov/nchs/nhanes/nh3data.htm", [".xpt",".dat",".sas",".txt",".pdf"], "ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/nhanes","E:\Ben\Research\Datasets\Life Science\\")


def browse_directory_tables(directory: str, extensions=[""]) -> [str]:
    file_names: [str] = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            for ext in extensions:
                if ext in file:
                    file_names.append(os.path.join(root, file))
    return file_names


def count_elements(directory: str, attr=[""], all: bool = False) -> ([], [], int, int):
    sequence = []
    columns = []
    cpt = 0
    total_size = 0

    not_included = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if ".XPT" in file:
                found = False
                if not all:
                    for a in attr:
                        if a in file:
                            found = True
                if (not found) and (not all):
                    not_included.append(file)
                else:
                    fileName = os.path.join(root, file)
                    print('Opening file', fileName)
                    df = pd.read_sas(fileName)
                    if 'SEQN' in df:
                        total_size = total_size + os.path.getsize(fileName)
                        cpt = cpt + 1
                        for c in list(df):
                            columns.append(c)
                        for s in df['SEQN'].values:
                            sequence.append(s)
                    else:
                        not_included.append(file)
    print("========================= Not included: ====================================")
    print(not_included)
    columns = list(dict.fromkeys(columns))
    sequence = list(dict.fromkeys(sequence))
    columns.sort()
    sequence.sort()

    return sequence, columns, total_size, cpt


def get_elements(sequence: [], columns: [], directory: str, attr, num_files: int = 0, all: bool = False) -> NAN:
    ls = len(sequence)
    lc = len(columns)
    data = np.empty((ls, lc))
    data[:] = np.NaN
    print("Loading Files")
    cpt = 0
    for root, dirs, files in os.walk(directory):
        for file in files:
            if ".XPT" in file:
                found = False
                if (not all):
                    for a in attr:
                        if a in file:
                            found = True
                if all or found:
                    file_name: str = os.path.join(root, file)
                    df = pd.read_sas(file_name)
                    all_columns: [str] = list(dict.fromkeys(list(df)))
                    if 'SEQN' in all_columns:
                        print('Reading file  ', cpt, "/", num_files, file_name)
                        cpt = cpt + 1
                        for index, row in df.iterrows():
                            sIndex = bisect.bisect_left(sequence, row['SEQN'])
                            for c in all_columns:
                                try:
                                    cIndex = bisect.bisect_left(columns, c)
                                    data[sIndex][cIndex] = row[c]
                                except ValueError:
                                    # print('Error:',row[c],type(row[c]), c, fileName)
                                    pass
    return data


def np_to_csv(data, columns: [], dest: str = 'e:/nhanesTestVeryFast3.csv'):
    header = ''
    for c in columns:
        header = header + c + ', '
    print("header")
    print(header)
    np.savetxt(dest, data, header=header, delimiter=', ', comments='')
    pass


def np_to_pandas(data, columns):
    df = pd.DataFrame(data, columns=columns)
    return df


def nhanes_merger_numpy(dir, attr=[""], dest='e:/nhanesF.csv', all=False):
    seqn, columns, totalSize, nbFiles = count_elements(dir, attr, all)
    ls = len(seqn)
    lc = len(columns)
    print("===> Database filtering info:  ( nb Part", ls, ') (nb Columns', lc, ') (total file size (MBs)',
          totalSize / 1024 / 1024, ') (nb Files)', nbFiles)
    data = get_elements(seqn, columns, dir, attr, nbFiles, all)
    # npToCSV(data,columns,dest)
    df = np_to_pandas(data, columns)
    df.to_csv(dest)
    return df


def load_csv(name, ageMin=-1, ageMax=200):
    df = pd.read_csv(name, low_memory=False)
    if 'RIDAGEYR' in df:
        l = [x and y for x, y in zip((df['RIDAGEYR'] >= ageMin), (df['RIDAGEYR'] <= ageMax))]
        return df[l]
    else:
        return df


def keep_non_null(df, col):
    l = (~df[col].isnull())
    return df[l]


def keep_equal(df, col, val):
    l = (df[col] == val)
    return df[l]


def keep_different(df, col, val):
    l = (df[col] != val)
    return df[l]


def keep_greater_than(df, col, val):
    l = (df[col] > val)
    return df[l]


def keep_greater_equal(df, col, val):
    l = (df[col] >= val)
    return df[l]


def keep_lower_than(df, col, val):
    l = (df[col] < val)
    return df[l]


def keep_lower_equal(df, col, val):
    l = (df[col] <= val)
    return df[l]


def keep_columns(df, cols):
    return df[cols]
