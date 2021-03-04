import os
import shutil
import time
import sys
import pandas


def readConfig():
    df_config = pandas.read_csv('ProductBook.csv', dtype=str)
    df_config.dropna(subset=['BIB_No'], inplace=True)
    df_config = df_config.replace(' ', '')
    return df_config


class DriveLog:
    df_config = readConfig()

    def __init__(self, log_path):

        self.path = os.path.dirname(os.path.realpath(sys.argv[0]))
        self.log_path = log_path
        self.columns = ['BIB_No', 'BIB_Type', 'Socket_Density', 'Pass_Code_Bin1',
                        'Bin_Sort_Index', 'Empty_Socket', 'ECID_Index', 'Split_ECID_method',
                        'Wafer_lot_Word1', 'Wafer_lot_Word2', 'Wafer_lot_Word3',
                        'Include_Bin2_check', 'Check_Type', 'Special_Words']
        self.dic_Bin2 = {}
        self.dic_Qcheck = {}
        self.df_config = DriveLog.df_config

    def readFolder(self, path):
        list = os.listdir(path)
        return list

    # def readConfig(self):
    #     self.df_config = pandas.read_csv('ProductBook.csv', dtype=str)
    #     self.df_config.dropna(subset=['BIB_No'], inplace=True)
    #     self.df_config = self.df_config.str.replace(' ','')
    #     return self.df_config

    # Get lot number
    def identifyLot(self):
        rows = self.rows
        for each in rows:
            if each.startswith('LOTID:'):
                lot = each.split(':')[1]
                return lot.replace(' ', '')

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

    # get all rows startswith lot number
    def getdata(self):

        self.rows_data = [each for each in self.rows if each.startswith(self.lot)]
        tmp1 = pandas.DataFrame(self.rows_data)
        # rows_data = tmp1[0].str.split(',', expand=True)

        key_list = ['Bin_Sort_Index', 'ECID_Index', 'Wafer_lot_Word1', 'Wafer_lot_Word2', 'Wafer_lot_Word3']
        index_list = []
        for each in key_list:
            if self.dic_Bin2[each]:
                index_list.append(self.dic_Bin2[each])

        tmp = [i.split(',') for i in self.rows_data]
        for i in tmp:
            i.remove('')
        rows_data = pandas.DataFrame(tmp)

        key_location = rows_data.columns[int(self.dic_Bin2['Key_Word_Location'])]
        rows_data = rows_data[rows_data[key_location].isin(index_list)]
        self.df_rows = pandas.DataFrame(rows_data)

        # tmp = [i.split(',') for i in self.rows_data]
        # tmp = [i for i in tmp if i[self.dic_Bin2['Key_Word_Location']] in index_list]
        #
        # # slot id
        # tmp = [i.append(5) for i in tmp]
        # tmp = [i[6:] for i in tmp]
        #
        # for i in tmp:
        #     i.remove('')

        return self.rows_data

    def readfile(self, file_path):
        fOpen = open(file_path, 'r')
        rows = fOpen.readlines()
        fOpen.close()

        self.rows = [each.replace('\n', '') for each in rows]
        # rows = [each.replace(' ', '') for each in rows]

        self.lot = self.identifyLot()

        self.bib_number, bib_index = self.identifyBib()
        self.df_bib = self.df_config[self.df_config['BIB_No'].apply(lambda row: row in self.bib_number)]

        self.identifyOven()

        df_Bin2 = self.df_bib[self.df_bib['Check_Type'] == 'Bin2']
        df_Qcheck = self.df_bib[self.df_bib['Check_Type'] == 'Qcheck']

        if len(df_Bin2):
            self.dic_Bin2 = df_Bin2.to_dict('records')[0]

            socket_location = self.dic_Bin2['1st_Socket_Location']
            socket_location = int(socket_location)
            if socket_location > 0:
                socket_location -= 1
            self.dic_Bin2['1st_Socket_Location'] = socket_location

        if len(df_Qcheck):
            self.dic_Qcheck = df_Qcheck.to_dict('records')[0]

            socket_location = self.dic_Qcheck['1st_Socket_Location']
            socket_location = int(socket_location)
            if socket_location > 0:
                socket_location -= 1
            self.dic_Qcheck['1st_Socket_Location'] = socket_location

        self.getdata()

    def process_result(self):

        self.readfile(self.log_path)

        result = pandas.DataFrame()
        result_Bin2check = pandas.DataFrame()
        result_Qcheck = pandas.DataFrame()

        if self.dic_Bin2 and self.dic_Bin2['Include_Bin2_check'] == 'Y':
            result_Bin2check = self.bin2check()
            result_Bin2check = pandas.concat([result_Bin2check, pandas.DataFrame(columns=['BI_Result(HardBin)'])])
        if self.dic_Qcheck:
            result_Qcheck = self.Qcheck()
            result_Qcheck['BI_Result(HardBin)'] = 'Qcheck'

        if len(result_Bin2check) and len(result_Qcheck):
            result = pandas.concat([result_Bin2check, result_Qcheck])
            result.drop_duplicates(subset=['Slot_ID', 'Socket_ID', 'Wafer_ID', 'Wafer_Lot', 'Die_X', 'Die_Y'],
                                   keep='first', inplace=True)

        elif len(result_Bin2check):
            result = result_Bin2check
        elif len(result_Qcheck):
            result = result_Qcheck

        result['Lot_ID'] = self.lot
        result['Oven_ID'] = self.oven
        result['BIB_ID'] = self.bib_number

        result = result[
            ['Lot_ID', 'Oven_ID', 'BIB_ID', 'Slot_ID', 'Socket_ID', 'Wafer_ID', 'Wafer_Lot', 'Die_X', 'Die_Y',
             'BI_Result(HardBin)']]
        self.result = result
        return result

    def blank_check(self, value):

        # if nan
        if value != value:
            return False

        # if not None,''
        elif value:
            return True

        else:
            return False

    def bin2check(self):

        df_ecid = self.get_ECID(self.dic_Bin2)

        # if not nan, None, ''
        if self.blank_check(self.dic_Bin2['Wafer_lot_Word1']):
            df_wafer = self.get_wafer(self.dic_Bin2)

        else:
            df_wafer = pandas.DataFrame()

        if len(df_wafer):
            df_result = pandas.merge(left=df_ecid, right=df_wafer, on='Slot_ID', how='outer')
        else:
            df_result = df_ecid

        df_binsort = self.get_binsort(self.dic_Bin2)
        if len(df_binsort):
            df_result = pandas.merge(left=df_result, right=df_binsort, on=['Slot_ID', 'Socket_ID'], how='outer')

        return df_result

    def Qcheck(self):

        df_ecid = self.get_ECID(self.dic_Qcheck)
        if not len(df_ecid):
            return pandas.DataFrame()

        # if not nan, None, ''
        if self.blank_check(self.dic_Bin2['Wafer_lot_Word1']):
            df_wafer = self.get_wafer(self.dic_Bin2)

        else:
            df_wafer = pandas.DataFrame()

        if len(df_wafer):
            df_result = pandas.merge(left=df_ecid, right=df_wafer, on='Slot_ID', how='outer')
        else:
            df_result = df_ecid

        return df_result

    # get bin result
    def get_binsort(self, dic={}):
        if not dic:
            dic = self.dic_Bin2
        key_location = int(dic['Key_Word_Location'])
        bin_index = dic['Bin_Sort_Index']
        socket_location = dic['1st_Socket_Location']
        socket_density = int(dic['Socket_Density'])

        bin_list_slot = []
        for each_row in self.rows_data:
            tmp = each_row.split(',')
            while '' in tmp:
                tmp.remove('')
            # todo: slot id - position 6, may need add to config file
            slot_id = tmp[5]
            if tmp[key_location] == bin_index:
                bin_row = tmp[socket_location:]
                bin_row_copy = bin_row[0:socket_density]
                list_bin = self.str2bin(bin_row_copy)
                df = pandas.DataFrame(list_bin)
                df['Slot_ID'] = slot_id

                bin_list_slot.append(df)

        df_result = pandas.concat(bin_list_slot)
        return df_result.drop_duplicates()

    # get ecid and slot per file
    def get_ECID(self, dic={}):

        if not dic:
            dic = self.dic_Bin2
        key_location = int(dic['Key_Word_Location'])
        socket_location = dic['1st_Socket_Location']
        ecid_index = dic['ECID_Index']
        socket_density = int(dic['Socket_Density'])

        ecid_list_slot = []

        for each_row in self.rows_data:
            tmp = each_row.split(',')
            while '' in tmp:
                tmp.remove('')
            # todo: slot id - position 6, may need add to config file
            slot_id = tmp[5]
            if tmp[key_location] == ecid_index:
                ecid_row = tmp[socket_location:]
                ecid_row_copy = ecid_row[0:socket_density]

                list_ecid = self.str2ecid(ecid_row_copy, dic)
                # dic_ecid = {
                #     'Slot_ID': slot_id,
                #     'ECID_list': list_ecid,
                # }
                df = pandas.DataFrame(list_ecid)
                df['Slot_ID'] = slot_id

                ecid_list_slot.append(df)

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

        # {'Slot_ID':'Wafer_ID'}
        dic_wafer_1 = {}
        dic_wafer_2 = {}
        dic_wafer_3 = {}

        for each_row in self.rows_data:
            tmp = each_row.split(',')
            while '' in tmp:
                tmp.remove('')
            # todo: slot id - position 6, may need add to config file
            slot_id = tmp[5]

            if tmp[key_location] == dic['Wafer_lot_Word1']:
                dic_wafer_1[slot_id] = self.str2waferlot(tmp) or 'Blank'

            elif tmp[key_location] == dic['Wafer_lot_Word2']:
                dic_wafer_2[slot_id] = self.str2waferlot(tmp) or 'Blank'

            elif tmp[key_location] == dic['Wafer_lot_Word3']:
                dic_wafer_3[slot_id] = self.str2waferlot(tmp) or 'Blank'

        df_wafer_1 = pandas.DataFrame(dic_wafer_1, index=['1'])
        df_wafer_2 = pandas.DataFrame(dic_wafer_2, index=['2'])
        df_wafer_3 = pandas.DataFrame(dic_wafer_2, index=['3'])
        df_wafer = pandas.concat([df_wafer_1, df_wafer_2, df_wafer_3])
        df_wafer = df_wafer.T.reset_index()
        df_wafer = df_wafer.rename(columns={'index': 'Slot_ID'})
        df_wafer['Wafer_Lot'] = df_wafer['1'] + df_wafer['2'] + df_wafer['3']
        df_wafer.drop(columns=['1', '2', '3'], inplace=True)
        return df_wafer

    # get ecid(DEC) from one row
    def str2bin(self, bin_row, dic={}):

        if not dic:
            dic = self.dic_Bin2
        bin1_code = dic['Pass_Code_Bin1']
        empty_socket = dic['Empty_Socket'].split(',')

        socket_id = 0
        list_bin_result = []
        for each in bin_row:
            socket_id += 1
            if each == bin1_code:
                dic_each = {'Socket_ID': socket_id,
                            'BI_Result(HardBin)': '1',
                            }
            elif each in empty_socket:
                continue
            else:
                dic_each = {'Socket_ID': socket_id,
                            'BI_Result(HardBin)': each,
                            }

            list_bin_result.append(dic_each)

        return list_bin_result

    # get ecid(DEC) from one row
    def str2ecid(self, ecid_row, dic={}):

        if not dic:
            dic = self.dic_Bin2
        split_ecid_method = dic['Split_ECID_method']
        empty_socket = dic['Empty_Socket'].split(',')

        if 'A' in split_ecid_method:
            wafer_start = 0
            x_start = -4
            y_start = -2



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
            if each not in empty_socket:
                try:
                    wafer_id = int(each[wafer_start:x_start])
                    x_id = (each[x_start:y_start])
                    y_id = (each[y_start:])

                    if 'A' not in split_ecid_method:
                        x_id = int(x_id, base=16)
                        y_id = int(y_id, base=16)

                    dic = {'Socket_ID': socket_id,
                           'Wafer_ID': wafer_id,
                           'Die_X': x_id,
                           'Die_Y': y_id
                           }
                    list_ecid.append(dic)
                except Exception as e:
                    print(self.lot, e)

        return list_ecid

    # get wafer lot from 3 wafer lot index
    def str2waferlot(self, rows, dic={}):

        if not dic:
            dic = self.dic_Bin2
        socket_density = dic['Socket_Density']
        empty_socket = dic['Empty_Socket'].split(',')
        socket_location = dic['1st_Socket_Location']

        # slot_id = rows[5]
        wafer_row = rows[socket_location:]
        wafer_row = wafer_row[0:int(socket_density)]

        wafer_row = [e for e in wafer_row if e not in empty_socket]
        if len(wafer_row):
            return wafer_row[0]
        else:
            return 0

    def to_csv(self, **kwargs):

        # result = self.process_result()
        if not kwargs:
            kwargs['path_or_buf'] = '{0}.csv'.format(self.lot)

        self.result.to_csv(**kwargs)


def main():
    log_path = 'D:\\NewECIDcheck\\LogFiles\\TCALYPSO100\\TJMEA2LLP401TTJ009SP4_DriverMonitor.log'
    log_path = 'D:\\NewECIDcheck\\LogFiles\\TCALYPSO100\\TJMEA2LLP401FSL015BIN2_DriverMonitor.log'
    log_path = 'D:\\NewECIDcheck\\LogFiles\\TCALYPSO100\\TJMEA2LLP401FSL004REB3_DriverMonitor.log'
    log_path = 'D:\\NewECIDcheck\\LogFiles\\KPANTHER257'

    for each in readFolder(log_path):
        # try:
        each_file = log_path + '\\' + each

        one = DriveLog(each_file)

        one.process_result()
        # one.to_csv('asfas')
        del one
    # except Exception as e:
    #     print(each,e)


def readFolder(path):
    list = os.listdir(path.replace('\n', ''))
    return list


if __name__ == '__main__':
    main()
