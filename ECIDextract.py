import os
import shutil
import time
import sys
import pandas


# import numpy as np


def readConfig():
    df_config = pandas.read_csv('ProductBook.csv', dtype=str)
    df_config.dropna(subset=['BIB_No'], inplace=True)
    df_config = df_config.replace([' ', '\t\t'], None)
    return df_config


class Err(Exception):
    pass


class DriveLog:
    df_config = readConfig()

    def __init__(self, log_path, bin2_path=''):

        self.path = os.path.dirname(os.path.realpath(sys.argv[0]))
        self.log_path = log_path
        self.columns = ['BIB_No', 'BIB_Type', 'Socket_Density', 'Pass_Code_Bin1',
                        'Bin_Sort_Index', 'Empty_Socket', 'ECID_Index', 'Split_ECID_method',
                        'Wafer_lot_Word1', 'Wafer_lot_Word2', 'Wafer_lot_Word3',
                        'Include_Check', 'Check_Type', 'Special_Words']
        self.dic_Bin2 = {}
        self.dic_Qcheck = {}
        self.dic_from_file = {}
        self.df_config = DriveLog.df_config
        self.bin2_path = bin2_path
        self.bin2_er = 'Not Processed yet.'
        self.rows_Bin2 = pandas.DataFrame()
        self.rows_Qcheck = pandas.DataFrame()
        self.data_flag = ''
        self.filter_blank_in_ECID = True

    def readFolder(self, path):
        list = os.listdir(path)
        return list

    # Get lot number
    def identifyLot(self):
        rows = self.rows
        for each in rows:
            if each.startswith('LOTID:'):
                lot = each.split(':')[1]
                return lot.replace(' ', '')

    # Get lot number
    def identifyblanksocket(self):
        rows = self.rows
        dic = {}
        dic_NRDS = {}

        for each in rows:

            if each.startswith('Slot'):
                blank_sockets = each.split(',')
                blank_sockets = [''.join(list(filter(str.isdigit, i))) for i in blank_sockets if i]

                if 'NRDS' in each and 'Marked bad' not in each:

                    if blank_sockets[0] in dic_NRDS.keys():
                        dic_NRDS[blank_sockets[0]] += blank_sockets[1:]
                    else:
                        dic_NRDS[blank_sockets[0]] = blank_sockets[1:]

                elif 'Marked bad' in each:

                    if blank_sockets[0] in dic.keys():
                        dic[blank_sockets[0]] += blank_sockets[1:]
                    else:
                        dic[blank_sockets[0]] = blank_sockets[1:]

        for each in dic:
            dic[each] = list(set(dic[each]))

        for each in dic_NRDS:
            dic_NRDS[each] = list(set(dic_NRDS[each]))

        self.dic_blank_sockets = dic
        self.dic_NRDS = dic_NRDS
        pass
        pass

    def identifyBib(self):
        rows = self.rows
        lot = self.lot

        for each in rows:
            if each.startswith(lot):
                list_1st_row = each.split(',')
                for bib in list_1st_row:
                    if bib.isdigit():
                        return bib, list_1st_row.index(bib)

    def identifyOven(self):

        rows = self.rows
        for each in rows:
            if each.startswith('System'):
                list_1st_row = each.split(' ')
                self.oven = list_1st_row[1]
                return self.oven

    def identifySpecialWords(self):

        rows = self.rows
        for each in rows:
            if 'Primary Diag:' in each:
                list_1st_row = each.split('Primary Diag:')[1]
                self.special_words = list_1st_row.split('.')[0]
                return self.special_words

    def getSpecialWords(self, df):

        if not len(df):
            return False

        col = filter(self.blank_check, df['Special_Words'])
        words = set(col)
        if words:
            return list(words)[0]
        else:
            return False

    # get all rows startswith lot number from bin2 log file
    def getbin2df(self):
        fOpen = open(self.bin2_path, 'r')
        ori_rows = fOpen.readlines()
        fOpen.close()

        rows = [each.replace('\n', '') for each in ori_rows]
        rows = [each.replace(' ', '').replace('=', '').replace('?', '') for each in rows if '###' in each]
        dic = {}

        if 'Socket_Density' in self.dic_Bin2.keys():
            if self.blank_check(self.dic_Bin2['Socket_Density']):
                socket_density = (self.dic_Bin2['Socket_Density'])

        elif 'Socket_Density' in self.dic_Qcheck.keys():
            if self.blank_check(self.dic_Qcheck['Socket_Density']):
                socket_density = self.dic_Qcheck['Socket_Density']

        for each in rows:
            tmp = each.split('###')
            bib = tmp[1]
            res = tmp[2].split(',')
            lis = []
            for i in res:
                if i.isalnum() and not i.isdigit():
                    bin2_result = i
                elif i.isdigit():
                    lis.append({'Bin2_Result': bin2_result,
                                'Socket_ID': int(i)})
            bad_socket = [i['Socket_ID'] for i in lis]
            lis_good = [{'Bin2_Result': 'pass', 'Socket_ID': i} for i in range(1, int(socket_density) + 1) if
                        i not in bad_socket]

            df = pandas.DataFrame(lis + lis_good)
            dic[bib] = df

        result = pandas.DataFrame()
        for key in dic.keys():
            df = dic[key]
            df['BIB_ID'] = key
            result = pandas.concat([result, df])

        # once good, always good
        df_pass = result[result['Bin2_Result'] == 'pass']
        result = pandas.concat([result, df_pass])
        result.drop_duplicates(subset=['BIB_ID', 'Socket_ID'], keep='last', inplace=True)

        return result

    def get_rows_bin2(self):
        bin2_start = ['Started']
        bin2_complete = ['Complete']
        # res = len(set(A).intersection(set(B)))
        rows = [each for each in self.rows if (each.startswith(self.lot)) or len([x for x in bin2_start if x in each])]

        rows_bin2 = []
        rows_qcheck = []
        bin2_flag = False

        for each in rows:
            if len([x for x in (bin2_start + bin2_complete) if x in each]):
                bin2_flag = not bin2_flag
            elif bin2_flag:
                rows_bin2.append(each)
            else:
                rows_qcheck.append(each)

        self.rows_Bin2 = rows_bin2
        self.rows_Qcheck = rows_qcheck

        return rows_bin2, rows_qcheck

    # get all rows startswith lot number
    def getdata(self):

        self.rows_data = [each for each in self.rows if each.startswith(self.lot)]
        # tmp1 = pandas.DataFrame(self.rows   _data)
        # rows_data = tmp1[0].str.split(',', expand=True)
        #
        # key_list = ['Bin_Sort_Index', 'ECID_Index', 'Wafer_lot_Word1', 'Wafer_lot_Word2', 'Wafer_lot_Word3']
        # index_list = []
        # for each in key_list:
        #     if self.dic_Bin2[each]:
        #         index_list.append(self.dic_Bin2[each])
        #
        # tmp = [i.split(',') for i in self.rows_data]
        # for i in tmp:
        #     i.remove('')
        # rows_data = pandas.DataFrame(tmp)
        #
        # key_location = rows_data.columns[int(self.dic_Bin2['Key_Word_Location'])]
        # rows_data = rows_data[rows_data[key_location].isin(index_list)]
        # self.df_rows = pandas.DataFrame(rows_data)

        return self.rows_data

    # Read index:
    #            Device, start Time, Log Time, BIB, Driver, Slot, VDUT1 Cur,VDUT3 Cur,VDUT1 Volt,VDUT3 Volt,
    #            Read DUT data0,Read DUT data1,Chamber Temp,Read DUT data2,Read DUT data3,Read DUT data4,Read DUT data5,
    #            Read DUT data6,Read DUT data7,Read DUT data8,Read DUT data9,ReadBIB_temp,Read DUT data0,Read DUT data1,
    def read_index(self):

        rows = self.rows
        list_index = ['Device', 'start Time', 'BIB', 'Driver', 'Slot']
        for each in rows:
            if all(key in each for key in list_index):
                tmp = each.replace(', ', ',')
                key_index = tmp.split(',')
                self.dic_from_file = {'BIB': key_index.index('BIB'),
                                      'Slot': key_index.index('Slot'),
                                      'Driver': key_index.index('Driver'), }

    def readfile(self, file_path):
        fOpen = open(file_path, 'r')
        rows = fOpen.readlines()
        fOpen.close()

        self.rows = [each.replace('\n', '') for each in rows]
        # rows = [each.replace(' ', '') for each in rows]

        self.lot = self.identifyLot()

        self.bib_number, bib_index = self.identifyBib()
        self.df_bib = self.df_config[self.df_config['BIB_No'].apply(lambda row: row in self.bib_number)]
        if not len(self.df_bib):
            raise Err('No related BIB board')

        self.identifyOven()
        self.identifyblanksocket()

        # read index from file
        self.read_index()

        df_Bin2 = self.df_bib[self.df_bib['Check_Type'] == 'Bin2']
        if self.getSpecialWords(df_Bin2):
            spwords = self.identifySpecialWords()
            df_Bin2 = df_Bin2[df_Bin2['Special_Words'].fillna('JustForProcess').apply(lambda row: row in spwords)]
            if not len(df_Bin2):
                df_Bin2 = self.df_bib[self.df_bib['Check_Type'] == 'Bin2']

        df_Qcheck = self.df_bib[self.df_bib['Check_Type'] == 'Qcheck']
        if self.getSpecialWords(df_Qcheck):
            spwords = self.identifySpecialWords()
            df_Qcheck = df_Qcheck[df_Qcheck['Special_Words'].fillna('JustForProcess').apply(lambda row: row in spwords)]
            if not len(df_Qcheck):
                df_Qcheck = self.df_bib[self.df_bib['Check_Type'] == 'Qcheck']

        if len(df_Bin2) and (df_Bin2['Include_Check'].iloc[0] == 'Y'):
            self.dic_Bin2 = df_Bin2.to_dict('records')[0]

            self.dic_Bin2['1st_Socket_Location'] = self.index_check(self.dic_Bin2['1st_Socket_Location'])
            self.dic_Bin2['Key_Word_Location'] = self.index_check(self.dic_Bin2['Key_Word_Location'])

        if len(df_Qcheck) and (df_Qcheck['Include_Check'].iloc[0] == 'Y'):
            self.dic_Qcheck = df_Qcheck.to_dict('records')[0]

            self.dic_Qcheck['1st_Socket_Location'] = self.index_check(self.dic_Qcheck['1st_Socket_Location'])
            self.dic_Qcheck['Key_Word_Location'] = self.index_check(self.dic_Qcheck['Key_Word_Location'])

        self.getdata()

    def process_result(self):

        columns_result = ['Lot_ID', 'Oven_ID', 'BIB_ID', 'Driver_ID', 'Slot_ID', 'Socket_ID', 'Wafer_Lot', 'ECID_BI',
                          'Wafer_ID', 'Die_X', 'Die_Y', 'BI_Result(HardBin)', 'Bin2_Result']

        self.readfile(self.log_path)
        self.get_rows_bin2()

        result = pandas.DataFrame()
        result_Bin2check = pandas.DataFrame()
        result_Qcheck = pandas.DataFrame()

        if len(self.rows_Bin2):
            self.data_flag = 'Bin2'

        if self.dic_Bin2 and self.dic_Bin2['Include_Check'] == 'Y':
            result_Bin2check = self.bin2check()
            result_Bin2check = pandas.concat([result_Bin2check, pandas.DataFrame(columns=['BI_Result(HardBin)'])])

        if len(self.rows_Qcheck):
            self.data_flag = 'Qcheck'

        if self.dic_Qcheck and self.dic_Qcheck['Include_Check'] == 'Y':
            result_Qcheck = self.Qcheck()
            result_Qcheck['BI_Result(HardBin)'] = 'Qcheck'

        if len(result_Bin2check) and len(result_Qcheck):
            # sometimes,bin2 has no wafer lot info,
            if 'Wafer_Lot' not in result_Bin2check.columns:
                result_Bin2check = pandas.merge(left=result_Bin2check,
                                                right=result_Qcheck[['BIB_ID', 'ECID_BI', 'Wafer_Lot']],
                                                on=['BIB_ID', 'ECID_BI'], how='left')
            else:
                temp = pandas.merge(left=result_Bin2check[['BIB_ID', 'ECID_BI', 'Wafer_Lot']],
                                    right=result_Qcheck[['BIB_ID', 'ECID_BI', 'Wafer_Lot']],
                                    on=['BIB_ID', 'ECID_BI'], how='left', suffixes=['', '_Q'])
                result_Bin2check['Wafer_Lot'] = temp.fillna(axis=1, method='bfill')['Wafer_Lot']

            result = pandas.concat([result_Bin2check, result_Qcheck])
            result.drop_duplicates(subset=['BIB_ID', 'Slot_ID', 'Socket_ID', 'Wafer_ID', 'ECID_BI', 'Die_X', 'Die_Y'],
                                   keep='first', inplace=True)

        elif len(result_Bin2check):
            result = result_Bin2check
        elif len(result_Qcheck):
            result = result_Qcheck
        else:
            raise Err('DM bin2 and Qcheck all fail')

        result['Oven_ID'] = self.oven

        # check bin2 log
        if self.bin2_path:
            try:
                bin2df = self.getbin2df()
                result = pandas.merge(left=result, right=bin2df, on=['BIB_ID', 'Socket_ID'], how='outer')
                self.bin2_er = 'Success'

            except Exception as e:
                er = 'Error: ' + str(e)
                self.bin2_er = er
                print(self.lot, 'Bin2 log erro: ', er)
        else:
            self.bin2_er = 'No related Bin2 log.'

        result['Lot_ID'] = self.lot
        # pop unprocessed field to prevent error
        # columns_result = [x for x in columns_result if x not in diff]
        diff = list(set(columns_result).difference(result.columns))

        for col in diff:
            result[col] = None

        result = result[columns_result]

        if self.filter_blank_in_ECID in [True, 'True']:
            result.dropna(axis=0, subset=['ECID_BI'], inplace=True)

        self.result = result

        return result, self.bin2_er

    # if not blank return true
    def blank_check(self, value, value_mode=False):

        # if nan
        if value != value:
            return False

        # if not None,''
        elif value:

            if value_mode:
                return value
            else:
                return True

        else:
            return False

    # change value from position to index of list : 8->7
    def index_check(self, value):

        value = int(value)
        if value > 0:
            value -= 1
        return value

    def bin2check(self):

        df_ecid = self.get_ECID(self.dic_Bin2)

        # if not nan, None, ''
        if self.blank_check(value=self.dic_Bin2['Wafer_lot_Word1']):
            df_wafer = self.get_wafer(self.dic_Bin2)

        else:
            df_wafer = pandas.DataFrame()

        if len(df_wafer):
            df_result = pandas.merge(left=df_ecid, right=df_wafer, on=['Slot_ID', 'Socket_ID'], how='left')
        else:
            df_result = df_ecid

        df_binsort = self.get_binsort(self.dic_Bin2)
        if len(df_binsort):
            df_result = pandas.merge(left=df_result, right=df_binsort,
                                     on=['Slot_ID', 'Socket_ID', 'BIB_ID', 'Driver_ID'],
                                     how='outer')

        return df_result

    def Qcheck(self):

        df_ecid = self.get_ECID(self.dic_Qcheck)
        if not len(df_ecid):
            return pandas.DataFrame()

        # if not nan, None, ''
        if self.blank_check(value=self.dic_Qcheck['Wafer_lot_Word1']):
            df_wafer = self.get_wafer(self.dic_Qcheck)

        else:
            df_wafer = pandas.DataFrame()

        if len(df_wafer):
            df_result = pandas.merge(left=df_ecid, right=df_wafer, on=['Slot_ID', 'Socket_ID'], how='left')
        else:
            df_result = df_ecid

        return df_result

    # get index from file
    def write_info(self, row, df=None):
        # add slot and bib to df column

        slot_index = 5
        BIB_index = 3
        driver_index = 4

        if self.dic_from_file:
            slot_index = self.dic_from_file['Slot']
            BIB_index = self.dic_from_file['BIB']
            driver_index = self.dic_from_file['Driver']

            slot_id = row[slot_index]
            BIB_id = row[BIB_index]
            driver_id = row[driver_index]

        if df:
            df['Slot_ID'] = slot_id
            df['BIB_ID'] = BIB_id
            df['Driver_ID'] = driver_id
            return df
        else:
            return slot_id, BIB_id, driver_id

        # get bin result

    def get_binsort(self, dic={}):
        if not dic:
            dic = self.dic_Bin2
        key_location = int(dic['Key_Word_Location'])
        bin_index = dic['Bin_Sort_Index']
        socket_location = dic['1st_Socket_Location']
        socket_density = int(dic['Socket_Density'])

        bin_list_slot = []

        if self.data_flag == 'Bin2':
            use_rows = self.rows_Bin2
        elif self.data_flag == 'Qcheck':
            use_rows = self.rows_Qcheck
        else:
            use_rows = self.rows_data

        for each_row in use_rows:
            tmp = each_row.split(',')
            while '' in tmp:
                tmp.remove('')

            if tmp[key_location] == bin_index:
                bin_row = tmp[socket_location:]
                bin_row_copy = bin_row[0:socket_density]

                # write slot and BIB
                slot_id, BIB_id, driver_id = self.write_info(row=tmp)
                list_bin = self.str2bin(bin_row_copy, slot_id)
                df = pandas.DataFrame(list_bin)
                df['Slot_ID'] = slot_id
                df['BIB_ID'] = BIB_id
                df['Driver_ID'] = driver_id

                bin_list_slot.append(df)
        if len(bin_list_slot):
            df_result = pandas.concat(bin_list_slot)

            # bin check may repeat multiple times and once good, see it as a good unit
            df_bin1 = df_result[df_result['BI_Result(HardBin)'] == 'pass']
            df_result = pandas.concat([df_bin1, df_result])

            return df_result.drop_duplicates(subset=['Slot_ID', 'Socket_ID', 'BIB_ID', 'Driver_ID'], keep='first')
        else:
            return pandas.DataFrame()

    # digital ecid info locates in one row
    def row2ecid(self, dic={}):

        if not dic:
            dic = self.dic_Bin2
        key_location = int(dic['Key_Word_Location'])
        socket_location = dic['1st_Socket_Location']
        ecid_index = dic['ECID_Index']
        socket_density = int(dic['Socket_Density'])

        ecid_list_slot = []

        if self.data_flag == 'Bin2':
            use_rows = self.rows_Bin2
        elif self.data_flag == 'Qcheck':
            use_rows = self.rows_Qcheck
        else:
            use_rows = self.rows_data

        for each_row in use_rows:
            tmp = each_row.split(',')
            while '' in tmp:
                tmp.remove('')

            if tmp[key_location] == ecid_index:
                ecid_row = tmp[socket_location:]
                ecid_row_copy = ecid_row[0:socket_density]

                # get slot, bib id
                slot_id, BIB_id, driver_id = self.write_info(row=tmp)
                list_ecid = self.str2ecid(ecid_row_copy, slot_id, dic)

                df = pandas.DataFrame(list_ecid)
                df['Slot_ID'] = slot_id
                df['BIB_ID'] = BIB_id
                df['Driver_ID'] = driver_id

                ecid_list_slot.append(df)

        return ecid_list_slot

    # for some Analog products,the ecid locates in 3position
    def trirows2ecid(self, dic={}):
        if not dic:
            dic = self.dic_Bin2
        key_location = int(dic['Key_Word_Location'])
        key_index = dic['ECID_Index'].split('&')
        empty_socket = dic['Empty_Socket'].split(',')

        # {'Slot_ID':'Wafer_ID'}
        dic_part_1 = {}
        dic_part_2 = {}
        dic_part_3 = {}

        dic_slot = {}

        if self.data_flag == 'Bin2':
            use_rows = self.rows_Bin2
        elif self.data_flag == 'Qcheck':
            use_rows = self.rows_Qcheck
        else:
            use_rows = self.rows_data

        for each_row in use_rows:
            tmp = each_row.split(',')
            while '' in tmp:
                tmp.remove('')
            # get slot, bib id
            slot_id, BIB_id, driver_id = self.write_info(row=tmp)
            dic_slot[slot_id] = slot_id, BIB_id, driver_id

            # if tmp[key_location] in empty_socket:
            #     pass

            if tmp[key_location] == key_index[0]:
                dic_part_1[slot_id] = self.get_rowdata(tmp) or 'Blank'

            elif tmp[key_location] == key_index[1]:
                dic_part_2[slot_id] = self.get_rowdata(tmp) or 'Blank'

            elif tmp[key_location] == key_index[2]:
                dic_part_3[slot_id] = self.get_rowdata(tmp) or 'Blank'

        ecid_list_slot = []
        for each_slot in dic_part_1.keys():
            tuple_rowsdata = zip(dic_part_1[each_slot], dic_part_2[each_slot], dic_part_3[each_slot])
            ecid_row_copy = list(map(lambda x: x[0] + x[1] + x[2], tuple_rowsdata))

            # get slot, bib id
            slot_id, BIB_id, driver_id = dic_slot[each_slot]
            list_ecid = self.str2ecid(ecid_row_copy, each_slot, dic)

            df = pandas.DataFrame(list_ecid)
            df['Slot_ID'] = slot_id
            df['BIB_ID'] = BIB_id
            df['Driver_ID'] = driver_id

            ecid_list_slot.append(df)

        return ecid_list_slot

    # get pure data from 1 row. not filter blank socket
    def get_rowdata(self, row, filter_blank=False, dic={}):

        if not dic:
            dic = self.dic_Bin2
        socket_density = dic['Socket_Density']
        socket_location = dic['1st_Socket_Location']
        empty_socket = dic['Empty_Socket'].split(',')

        row_data = row[socket_location:]
        row_data = row_data[0:int(socket_density)]

        if filter_blank:
            row_data = [e for e in row_data if e not in empty_socket]
        if len(row_data):
            return row_data
        else:
            return [0]

    # get ecid and slot per file
    def get_ECID(self, dic={}):

        if not dic:
            dic = self.dic_Bin2

        # ecid from 1 or 3 rows
        ecid_index = dic['ECID_Index']
        if len(ecid_index.split('&')) == 1:
            ecid_list_slot = self.row2ecid(dic)
        else:
            ecid_list_slot = self.trirows2ecid(dic)

        # reformat list -> pandas
        if len(ecid_list_slot) > 1:
            df_result = pandas.concat(ecid_list_slot)
        elif len(ecid_list_slot) == 1:
            df_result = ecid_list_slot[0]
        else:
            df_result = pandas.DataFrame()

        return df_result.drop_duplicates()

    # get wafer and slot per file

    def get_wafer(self, dic={}):
        if not dic:
            dic = self.dic_Bin2
        key_location = int(dic['Key_Word_Location'])
        key_index = [dic['Wafer_lot_Word1'], dic['Wafer_lot_Word2'], dic['Wafer_lot_Word3']]
        empty_socket = dic['Empty_Socket'].split(',')

        # {'Slot_ID':'Wafer_ID'}
        dic_part_1 = {}
        dic_part_2 = {}
        dic_part_3 = {}

        dic_slot = {}

        if self.data_flag == 'Bin2':
            use_rows = self.rows_Bin2
        elif self.data_flag == 'Qcheck':
            use_rows = self.rows_Qcheck
        else:
            use_rows = self.rows_data

        for each_row in use_rows:
            tmp = each_row.split(',')
            while '' in tmp:
                tmp.remove('')
            # get slot, bib id
            slot_id, BIB_id, driver_id = self.write_info(row=tmp)
            dic_slot[slot_id] = slot_id, BIB_id, driver_id

            # if tmp[key_location] in empty_socket:
            #     pass

            if tmp[key_location] == key_index[0]:
                dic_part_1[slot_id] = self.get_rowdata(tmp, dic=dic) or 'Blank'

            elif tmp[key_location] == key_index[1]:
                dic_part_2[slot_id] = self.get_rowdata(tmp, dic=dic) or 'Blank'

            elif tmp[key_location] == key_index[2]:
                dic_part_3[slot_id] = self.get_rowdata(tmp, dic=dic) or 'Blank'

        d1 = pandas.DataFrame(dic_part_1)
        if self.blank_check(key_index[1]):
            d2 = pandas.DataFrame(dic_part_2)
        else:
            d2 = pandas.DataFrame()
        if self.blank_check(key_index[2]):
            d3 = pandas.DataFrame(dic_part_3)
        else:
            d3 = pandas.DataFrame()

        d0 = pandas.concat([d1, d2, d3])
        # d0['Socket_ID']=d0.index + 1
        d0 = d0.fillna('Blank')
        b = pandas.DataFrame()
        for each in set(d0.index):
            tmp = d0[d0.index == each].reset_index(drop=True)
            a = tmp.apply(lambda x: ''.join(i for i in list(x)), axis=0)
            a = pandas.DataFrame(a, columns=['Wafer_Lot'])
            a['Slot_ID'] = a.index
            a['Socket_ID'] = each + 1
            b = pandas.concat([b, a])

        return b

        ecid_list_slot = []
        common_slots = set(dic_part_1) & set(dic_part_2) & set(dic_part_3)
        for each_slot in common_slots:
            if each_slot == '60':
                aaaa = 1
            tuple_rowsdata = zip(dic_part_1[each_slot], dic_part_2[each_slot], dic_part_3[each_slot])
            ecid_row_copy = list(map(lambda x: x[0] + x[1] + x[2], tuple_rowsdata))

            # get slot, bib id
            slot_id, BIB_id, driver_id = dic_slot[each_slot]

            df = pandas.DataFrame(ecid_row_copy, columns=['Wafer_Lot'])
            df['Socket_ID'] = df.index + 1
            df['Slot_ID'] = slot_id
            # df['BIB_ID'] = BIB_id
            # df['Driver_ID'] = driver_id

            ecid_list_slot.append(df)

        return pandas.concat(ecid_list_slot)

    # get ecid(DEC) from one row
    def str2bin(self, bin_row, slot_id, dic={}):

        # empty sockets index
        if slot_id in self.dic_blank_sockets:
            blank_sockets = self.dic_blank_sockets[slot_id]
        else:
            blank_sockets = {}

        if not dic:
            dic = self.dic_Bin2
        bin1_code = dic['Pass_Code_Bin1']
        # empty sockets code
        empty_socket = dic['Empty_Socket'].split(',')

        socket_id = 0
        list_bin_result = []
        for each in bin_row:
            socket_id += 1
            if each == bin1_code:
                dic_each = {'Socket_ID': socket_id,
                            'BI_Result(HardBin)': 'pass',
                            }

            elif each in empty_socket:
                continue

            # blank sockets from basic slot info
            elif str(socket_id) in blank_sockets:
                continue

            else:
                dic_each = {'Socket_ID': socket_id,
                            'BI_Result(HardBin)': each,
                            }

            list_bin_result.append(dic_each)

        return list_bin_result

    # get ecid(DEC) from one row
    def str2ecid(self, ecid_row_ori, slot_id, dic={}):
        ecid_row = ecid_row_ori.copy()
        if not dic:
            dic = self.dic_Bin2
        split_ecid_method = dic['Split_ECID_method']
        empty_socket = dic['Empty_Socket'].split(',')
        if slot_id in self.dic_blank_sockets:
            blank_sockets = self.dic_blank_sockets[slot_id]
        else:
            blank_sockets = {}

        # sample: 142205 / 42205,14/4: Wafer ID(Dec)
        if 'A' in split_ecid_method:
            wafer_start = 0
            x_start = -4
            y_start = -2
        # sample: 2819FF0F
        elif 'C' in split_ecid_method:
            ecid_row = [each[4:6]+each[2:4]+each[0:2]+each[6:] for each in ecid_row]
            wafer_start = 2
            x_start = 4
            y_start = 6

        elif split_ecid_method.isdigit():

            if int(split_ecid_method) == 6:
                wafer_start = 2
                x_start = 4
                y_start = 6
            elif int(split_ecid_method) == 8:
                wafer_start = 0
                x_start = 2
                y_start = 5

        socket_id = 0
        list_ecid = []
        for each in ecid_row:
            socket_id += 1
            if each in empty_socket:
                continue
            elif str(socket_id) in blank_sockets:
                continue
            else:
                # some code can not be read
                try:
                    # if 1:
                    # if each[wafer_start:x_start] == 'FF':
                    #     a1 = 1
                    wafer_id = int(each[wafer_start:x_start])
                    x_id = (each[x_start:y_start])
                    y_id = (each[y_start:])

                    if 'A' not in split_ecid_method:
                        x_id = int(x_id, base=16)
                        y_id = int(y_id, base=16)

                    dic_one = {'ECID_BI': ecid_row_ori[socket_id-1],
                               'Socket_ID': socket_id,
                               'Wafer_ID': wafer_id,
                               'Die_X': x_id,
                               'Die_Y': y_id
                               }
                    list_ecid.append(dic_one)
                except Exception as e:
                    print(self.lot, e)

        return list_ecid

    def to_csv(self, ouput_folder=None):

        # result = self.process_result()
        if not ouput_folder:
            ouput_file = 'D:\\NewECIDcheck\\folder1\\{0}.csv'.format(self.lot)
        else:
            ouput_file = ouput_folder + '\\{0}.csv'.format(self.lot)

        self.result.to_csv(ouput_file, index=False)


def main():
    config_tuple = folderConfig()
    log_path = config_tuple[0]
    ouput_folder = config_tuple[1]
    error_folder = config_tuple[2]
    filter_blank_in_ECID = config_tuple[3]
    del_processed_log = config_tuple[4]
    del_error_log = config_tuple[5]
    del_drive_noBin2_log = config_tuple[6]
    # log_path = 'D:\\NewECIDcheck\\LogFiles\\TCALYPSO100\\TJMEA2LLP401TTJ009SP4_DriverMonitor.log'
    # log_path = 'D:\\NewECIDcheck\\LogFiles\\TCALYPSO100\\TJMEA2LLP401FSL015BIN2_DriverMonitor.log'
    # log_path = 'D:\\NewECIDcheck\\LogFiles\\\\Test1'
    # log_path = 'D:\\NewECIDcheck\\LogFiles\\TCALYPSO100\\TJMEA2LLP401FSL004REB3_DriverMonitor.log'
    # log_path = 'D:\\NewECIDcheck\\LogFiles\\TPACE6'
    # log_path = 'E:\\EkkoWang\\ECIDcheck\\Driver Monitor - Copy'
    # log_path = 'D:\\NewECIDcheck\\folder1'
    # log_path = 'D:\\NewECIDcheck\\New folder\\LJ4LT41HOF00FSL005REB1SP1_DriverMonitor.log'

    # if '.log' in log_path:

    # one = DriveLog(log_path)
    #
    # one.process_result()
    #
    # one.to_csv()
    # del one
    folder = readFolder(log_path)
    log = []
    each_file = ''
    ld_log = ''
    for each in folder:

        if 'DriverMonitor' in each:

            now = time.strftime("%Y-%m-%d %H-%M-%S", time.localtime(time.time()))
            try:
                # if 1:
                each_file = log_path + '\\' + each
                # bin2_list = list(filter(lambda x: ((each.split('_')[0] + '.Log') in x) and (len(x.split('_'))==3), folder))
                bin2_list = list(filter(lambda x: (each.split('_')[0] in x) and x.startswith('B2_'), folder))
                if bin2_list:
                    ld_log = log_path + '\\' + bin2_list[0]
                else:
                    ld_log = ''
                # ld_log = ''

                one = DriveLog(each_file, ld_log)
                one.filter_blank_in_ECID = filter_blank_in_ECID

                one.process_result()

                one.to_csv(ouput_folder)

                log.append({'File': each,
                            'DM Result': 'Success',
                            'Bin2 Result': one.bin2_er,
                            'Time': now})

                if del_processed_log in ['True', True]:
                    if ld_log and (ld_log not in ['', ' ']):
                        dellogfile(ld_log)
                    else:
                        if del_drive_noBin2_log in ['True', True]:
                            dellogfile(each_file)
                        else:
                            pass

            except Exception as e:
                er = 'Error: ' + str(e)
                if del_error_log in ['True', True]:
                    dellogfile(each_file)
                    dellogfile(ld_log)
                else:
                    movelogfile(each_file, error_folder)
                    movelogfile(ld_log, error_folder)
                log.append({'File': each,
                            'Result': er,
                            'Bin2 Result': one.bin2_er,
                            'Time': now})
            if 'one' in locals().keys():
                del one
        elif each.startswith('LotReport'):
            each_file = log_path + '\\' + each
            dellogfile(each_file)
            # # add mode to csv
    now = time.strftime("%Y-%m-%d %H-%M-%S", time.localtime(time.time()))
    pandas.DataFrame(log).to_csv(error_folder + '\\' + 'ProcessLog-%s.csv' % now, mode='a', index=False)
    # getAddedfiles(log_path, ouput_folder)


def readFolder(path):
    list = os.listdir(path.replace('\n', ''))
    return list


def folderConfig():
    path = os.path.dirname(os.path.realpath(sys.argv[0]))
    fopen = open(path + '\\config.txt', 'r')
    lines = fopen.readlines()
    filter_blank_in_ECID = 'True'
    del_processed_log = 'True'

    for row in lines:
        row = row.rstrip('\n')
        if row.startswith('BI_log_input_folder'):
            input_folder = row.split('|')[1]
        if row.startswith('BI_csv_output_folder'):
            output_folder = row.split('|')[1]
        if row.startswith('BI_log_error_folder'):
            error_folder = row.split('|')[1]
        if row.startswith('filter_blank_in_ECID'):
            filter_blank_in_ECID = row.split('|')[1]
        if row.startswith('del_processed_log'):
            del_processed_log = row.split('|')[1]
        if row.startswith('del_error_log'):
            del_error_log = row.split('|')[1]
        if row.startswith('del_drive_noBin2_log'):
            del_drive_noBin2_log = row.split('|')[1]

    fopen.close()

    return (input_folder, output_folder, error_folder, filter_blank_in_ECID, del_processed_log, del_error_log,
            del_drive_noBin2_log)


def getAddedfiles(path_to_watch, ouput_folder):
    import os, time

    before = dict([(f, None) for f in os.listdir(path_to_watch)])
    while 1:
        time.sleep(10)
        after = dict([(f, None) for f in os.listdir(path_to_watch)])
        added = [f for f in after if not f in before]
        removed = [f for f in before if not f in after]
        if added:
            print("Added: ", ", ".join(added))
        if removed:
            print("Removed: ", ", ".join(removed))
        before = after


def movelogfile(src_path, dst_path):
    try:
        shutil.move(src_path, dst_path)
    except Exception as e:
        print(e, 'Error:Move log file')


def dellogfile(src_path):
    try:
        os.remove(src_path)
    except Exception as e:
        print(e, 'Error:Delete log file')


if __name__ == '__main__':
    try:
        main()
        print('Finished.')
        time.sleep(10)

    except Exception as e:
        print(e)
        time.sleep(10)
