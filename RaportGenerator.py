import xlsxwriter
import time
from multiprocessing import Process
from gzip import open as go
import xml.etree.ElementTree as ET
from random import randint
import os

headers_list = ['MSC File ID',
                'Open File',
                'MSC File Type',
                'MSC File CCY',
                'MSC File Hedge',
                'Fund CCY (nominal)',
                'Fund InstID',
                'SCL InstID',
                'ISIN',
                'Share Class Name',
                'Lunch Date',
                'Share Class ID',
                'Share Class Cat.Code',
                'Share CLass Cat.Name',
                'Valor',
                'Fund Name']


class DataGenerator(object):
    def __init__(self, source):
        self.file_tree = os.walk(source)

    def _generate_list_for_data_feching(self):
        for root, dir, files in self.file_tree:
            for item in files:
                if item.split('.')[-1] == 'gz':
                    file_list.insert(-1, item)


class ChunkData(object):
    def __init__(self):
        self.temp_list_data = []
        self.count_donw = 0
        for item in file_list:
            if self.count_donw <= 1:
                self.temp_list_data.append(item)
                self.count_donw += 1
                file_list.remove(item)
            else:
                break

    def _returnData(self):
        return self.temp_list_data


def _write_headers(headers):
    col = 0
    for obj in headers:
        worksheet.set_column(0, col, len(obj) + 10)
        worksheet.write(0, col, obj, bold)
        col += 1


def _write_rest_body(row, items):
    worksheet.write(row, 0, items[0])
    worksheet.write_url(row, 1, items[1], string='File Location')
    col = 2
    for item in items[2:]:
        worksheet.write(row, col, item)
        col += 1


def _parser(path, name, dump):
    try:
        file = go(path + '\\' + name)
        read = file.read()
        file.close()
        names = []
        msc_type_list = []
        msc_Hedge_list = []
        FundsInstID_list = []
        path_list = []
        ISIN_list = []
        Valor_list = []
        SCLinstID_list = []
        LaunchDate_list = []
        FundCCYnominal_list = []
        ShareClassCat_Code_list = []
        ShareClassCat_Name_list = []
        ShareClassName_list = []
        ShareClassCurrency_list = []
        ShareCLassId_list = []
        FundNames_list = []
        with go(path + '\\' + name) as fh:
            if len(read) < 250000000:
                mb = len(read) * 0.000001
                print('{}, {}MB'.format(name, round(mb, 2)))
                instrumnet_for_data = []
                funds_features = []
                en_funds = []
                tree = ET.parse(fh)
                root = tree.getroot()
                instruments = tree.findall('Instrument')
                attrib_dict = root.attrib
                instrument_type_Fund = instruments[0].attrib
                fund_names = instruments[0].findall('FundFeatures')
                # print '========================='
                for item in fund_names:
                    if item.attrib.get('language') == 'en':
                        for n in range(len(instruments) - 1):
                            Fund_name = item.find('FundName/Content').text
                            FundNames_list.append(Fund_name)
                for itr in range(len(instruments) - 1):
                    names.append(str(name).split('_')[1])
                    msc_type_list.append(attrib_dict['fileGroupData'])
                    msc_Hedge_list.append(attrib_dict['fileHedge'])
                    FundsInstID_list.append(int(instrument_type_Fund['instrumentId']))
                    path_list.append(path + '\\' + name)
                for i in instruments:
                    if i.attrib.get('type') == 'ShareClass':
                        instrumnet_for_data.append(i)
                        SCLinstID = i.attrib['instrumentId']
                        SCLinstID_list.append(int(SCLinstID))
                for obj in instrumnet_for_data:
                    funds_features.append(obj.findall('FundFeatures'))
                for obj in funds_features:
                    for item in obj:
                        if item.attrib.get('language') == 'en':
                            en_funds.append(item)
                for obj in en_funds:
                    if obj.findall('Identifier') == []:
                        ISIN_list.append('None')
                    if obj.findall('Identifier') == []:
                        Valor_list.append('None')
                    if obj.findall('LaunchDate') == []:
                        LaunchDate_list.append('None')
                    if obj.findall('NominalCurrency') == []:
                        FundCCYnominal_list.append('None')
                    if obj.findall('ShareClassCategory') == []:
                        ShareClassCat_Code_list.append('None')
                        ShareClassCat_Name_list.append('None')
                    if obj.findall('ShareClassName') == []:
                        ShareClassName_list.append('None')
                    if obj.findall('ShareClassCurrency') == []:
                        ShareClassCurrency_list.append('None')
                    if obj.findall('ShareClassId') == []:
                        ShareCLassId_list.append('None')
                    for item in obj:
                        if item.tag == 'Identifier':
                            if item.attrib.get('type') == 'ISIN':
                                ISIN_list.append(item.find('Content').text)
                            if item.attrib.get('type') == 'Securities number':
                                Valor_list.append(item.find('Content').text)
                        if item.tag == 'LaunchDate':
                            LaunchDate_list.append(item.find('Content').text)
                        if item.tag == 'NominalCurrency':
                            FundCCYnominal_list.append(item.find('Content').text)
                        if item.tag == 'ShareClassCategory':
                            sharecategory = item.findall('Content/')
                            ShareClassCat_Code_list.append(sharecategory[0].text)
                            ShareClassCat_Name_list.append(sharecategory[1].text)
                        if item.tag == 'ShareClassName':
                            ShareClassName_list.append(item.find('Content').text)
                        if item.tag == 'ShareClassCurrency':
                            ShareClassCurrency_list.append(item.find('Content').text)
                        if item.tag == 'ShareClassId':
                            ShareCLassId_list.append(item.text)
                final_list = zip(names, path_list, msc_type_list, ShareClassCurrency_list,
                                 msc_Hedge_list, FundCCYnominal_list, FundsInstID_list, SCLinstID_list,
                                 ISIN_list, ShareClassName_list, LaunchDate_list,
                                 ShareCLassId_list, ShareClassCat_Code_list,
                                 ShareClassCat_Name_list, Valor_list, FundNames_list)

                random = randint(0, 2000)
                for n in final_list:
                    if n is not None:
                        A = str(n).replace("u'", "").replace("\u", "").replace("(", '').replace(')', '').replace("'",
                                                                                                                 "")
                        os.system('echo "{}" >> {}\{}_{}.txt'.format(A, dump, random, name))

                    else:
                        continue
                exit(0)
            else:
                print("Skipping file to large, Exiting proccess")
                exit(0)
    except MemoryError as error:
        print(error)


if __name__ == '__main__':
    input_month = str(input('Podaj numer miesiaca: '))
    data_today = time.strftime('%Y-%m-%d')
    start = time.time()
    path = '.\MSC_{}'.format(input_month)
    extracted_path = r'.\MSC_{}'.format(
        input_month)
    txtdump = r'.\txtDump'
    row = 1
    file_list = []
    workbook = xlsxwriter.Workbook('Share Class Lis - {}.xlsx'.format(data_today))
    worksheet = workbook.add_worksheet('Test Raport')
    bold = workbook.add_format({'bold': True, 'bg_color': '#7CB9E8'})
    worksheet.autofilter("A1:P1")
    data = DataGenerator(extracted_path)
    data._generate_list_for_data_feching()
    total = len(file_list)
    _write_headers(headers_list)
    jobs = []
    jobs_len = []
    while file_list != []:
        print(total - len(file_list))
        if len(jobs) < 4:
            temp = ChunkData()
            data = temp._returnData()
            for name in data:
                final_list = Process(target=_parser, args=(extracted_path, name, txtdump))
                final_list.start()
                jobs.append(final_list)
                jobs_len.append('a')
        else:
            print('wating for job to ends')
            time.sleep(3)
            jobs = []
    if total == len(jobs_len):
        time.sleep(29)
        for item in [files for root, dirs, files in os.walk(txtdump)][0]:
            with open(txtdump + "\\" + item) as fh:
                readed = fh.readlines()
                for n in readed:
                    n = n.replace('"', '')
                    n = n.split(',')
                    if n[0].isalnum():
                        n[0] = str(n[0])
                    if n[0].isdigit():
                        n[0] = int(n[0])
                    n[6] = int(n[6])
                    n[7] = int(n[7])
                    _write_rest_body(row, n)
                    print('Writing data to excel: {}'.format(n[0]))
                    row += 1
    workbook.close()
    end = time.time()
    print(end - start)
