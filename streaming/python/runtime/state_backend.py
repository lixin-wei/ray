import logging
import os
from abc import ABC, abstractmethod
from os import path

from ray.streaming.config import ConfigHelper
from ray.streaming.constants import StreamingConstants

logger = logging.getLogger(__name__)


class StateBackend(ABC):

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def put(self, key, value):
        pass

    @abstractmethod
    def remove(self, key):
        pass


class MemoryStateBackend(StateBackend):

    def __init__(self, conf):
        self.__dic = dict()

    def get(self, key):
        return self.__dic.get(key)

    def put(self, key, value):
        self.__dic[key] = value

    def remove(self, key):
        if key in self.__dic:
            del self.__dic[key]


class LocalFileStateBackend(StateBackend):

    def __init__(self, conf):
        self.__dir = ConfigHelper.get_cp_local_file_root_dir(conf)
        logger.info("Start init local file state backend, root_dir={}.".format(self.__dir))
        try:
            os.mkdir(self.__dir)
        except FileExistsError:
            logger.info("dir already exists, skipped.")

    def put(self, key, value):
        logger.info("Put value of key {} start.".format(key))
        with open(self.__gen_file_path(key), "wb") as f:
            f.write(value)

    def get(self, key):
        logger.info("Get value of key {} start.".format(key))
        full_path = self.__gen_file_path(key)
        if not os.path.isfile(full_path):
            return None
        with open(full_path, "rb") as f:
            return f.read()

    def remove(self, key):
        logger.info("Remove value of key {} start.".format(key))
        os.remove(self.__gen_file_path(key))

    def rename(self, src, dst):
        logger.info("rename {} to {}".format(src, dst))
        os.rename(self.__gen_file_path(src), self.__gen_file_path(dst))

    def exists(self, key) -> bool:
        return os.path.exists(key)

    def __gen_file_path(self, key):
        return path.join(self.__dir, key)


class AtomicFsStateBackend(LocalFileStateBackend):

    def __init__(self, conf):
        super().__init__(conf)
        self.__tmp_flag = "_tmp"

    def put(self, key, value):
        tmp_key = key + self.__tmp_flag
        if super().exists(tmp_key) and not super().exists(key):
            super().rename(tmp_key, key)
        super().put(tmp_key, value)
        super().remove(key)
        super().rename(tmp_key, key)

    def get(self, key):
        tmp_key = key + self.__tmp_flag
        if super().exists(tmp_key) and not super().exists(key):
            return super().get(tmp_key)
        return super().get(key)

    def remove(self, key):
        tmp_key = key + self.__tmp_flag
        if super().exists(tmp_key):
            super().remove(tmp_key)
        super().remove(key)


class StateBackendFactory:

    @staticmethod
    def get_state_backend(worker_config) -> StateBackend:
        backend_type = ConfigHelper.get_cp_state_backend_type(worker_config)
        state_backend = None
        if backend_type == StreamingConstants.CP_STATE_BACKEND_LOCAL_FILE:
            state_backend = AtomicFsStateBackend(worker_config)
        elif backend_type == StreamingConstants.CP_STATE_BACKEND_MEMORY:
            state_backend = MemoryStateBackend(worker_config)
        return state_backend
