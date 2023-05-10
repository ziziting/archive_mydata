# -*- coding: UTF-8 -*-
import os
import configparser

class DataArchiveConfig:
    def __init__(self, cfg_file):
        self.cfg_file = cfg_file
        self.__check_config_file_exists()
        self.__cfg = None
        self.__init_config_file()
        self.__cfg_list = []
        self.__parse()

    def __check_config_file_exists(self):
        if not os.path.isfile(self.cfg_file):
            print(f"config file {self.cfg_file} not found.")

    def __init_config_file(self):
        try:
            with open(self.cfg_file, 'r') as fr:
                self.__cfg = configparser.ConfigParser()
                self.__cfg.read(self.cfg_file)
        except Exception as e:
            print(f"read config file {self.cfg_file} error. ERROR: {e}")

    def __parse(self):
        for section_name in self.__cfg.sections():
            opts = self.__cfg.options(section_name)
            configs = {}
            for opt in opts:
                k = opt
                v = self.__cfg.get(section_name, opt).split(",") if k.endswith("_list") else self.__cfg.get(section_name, opt)
                configs[k] = v
            self.__cfg_list.append(configs)

    def get_configs(self):
        return self.__cfg_list

    def add_config(self):
        pass

# data_sync_cfg = DataArchiveConfig("data_increment.cfg")
# cc = data_sync_cfg.get_configs()
# for l in cc:
#     print(l)