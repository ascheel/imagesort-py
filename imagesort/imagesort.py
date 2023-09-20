import sys
import os
import logging
import yaml
import argparse
from PIL import Image, ExifTags
import re
import sqlite3
import datetime
import hashlib
import shutil
from exiftool import ExifToolHelper
import json


class DB:
    def __init__(self, **kwargs):
        self.log = kwargs.get('log')
        self.db_file = os.path.splitext(os.path.abspath(__file__))[0] + ".db"
        self.db = sqlite3.connect(
            self.db_file,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        self._init_db()

    def _init_db(self):
        cur = self.db.cursor()
        sql = """
        CREATE TABLE IF NOT EXISTS
            settings (
                setting CHAR UNIQUE,
                value CHAR
            )
        """
        self.log.debug(f'Executing: \n{sql}')
        cur.execute(sql)

        sql = """
        CREATE TABLE IF NOT EXISTS
            camera (
                name CHAR UNIQUE,
                model CHAR UNIQUE,
                description CHAR,
                make CHAR
            )
        """
        self.log.debug(f'Executing: \n{sql}')
        cur.execute(sql)
        sql = """
        CREATE TABLE IF NOT EXISTS
            media (
                filename_original CHAR,
                filename_new CHAR UNIQUE,
                sha256sum CHAR UNIQUE,
                size INT,
                create_date TIMESTAMP,
                camera_id INT
            )
        """
        self.log.debug(f'Executing: \n{sql}')
        cur.execute(sql)
        default = os.path.join(os.path.expanduser("~"), "pictures")
        while not self.get_destination():
            destination = input(f"Directory to store images [{default}]: ") or default
            choice = input(f"Correct? ({destination}) [Y]/N: ").lower() or "y"
            if choice not in ('y', 'yes'):
                continue
            sql = "INSERT INTO settings (setting, value) VALUES (?, ?)"
            params = ("destination", destination)
            self.log.debug(f'Executing: \n{sql}')
            self.log.debug(f'  with params: {params}')
            cur.execute(sql, params)

        self.db.commit()

    def get_setting(self, setting):
        sql = "SELECT value FROM settings WHERE setting = ?"
        params = (setting, )
        cur = self.db.cursor()
        self.log.debug(f'Executing: \n{sql}')
        self.log.debug(f'  with params: {params}')
        cur.execute(sql, params)
        results = cur.fetchone()
        if not results:
            return None
        else:
            return results[0]

    def get_destination(self):
        return self.get_setting("destination")

    def camera_model_exists(self, model):
        sql = "SELECT count(*) FROM camera WHERE model = ?"
        params = (model, )
        cur = self.db.cursor()
        self.log.debug(f'Executing: \n{sql}')
        self.log.debug(f'  with params: {params}')
        cur.execute(sql, params)
        return cur.fetchone()[0] != 0

    def add_camera(self, **kwargs):
        _make  = kwargs.get("make")
        _model = kwargs.get("model")
        _name  = kwargs.get("name")
        _desc  = kwargs.get("desc")

        sql    = "INSERT INTO camera (make, model, name, description) VALUES (?, ?, ?, ?)"
        params = (_make, _model, _name, _desc)
        cur    = self.db.cursor()
        self.log.debug(f'Executing: \n{sql}')
        self.log.debug(f'  with params: {params}')
        cur.execute(sql, params)
        self.db.commit()

    def _get_camera_name_from_model(self, model):
        cur = self.db.cursor()

        sql = "SELECT name FROM camera WHERE model = ?"
        params = (model, )

        self.log.debug(f'Executing: \n{sql}')
        self.log.debug(f'  with params: {params}')
        cur.execute(sql, params)
        result = cur.fetchone()[0]
        return result

    def camera_id_from_model(self, model):
        cur = self.db.cursor()

        sql = "SELECT rowid FROM camera WHERE model = ?"
        params = (model, )

        self.log.debug(f'Executing: \n{sql}')
        self.log.debug(f'  with params: {params}')
        cur.execute(sql, params)
        result = cur.fetchone()
        return result[0]

    def get_camera_from_id(self, camera_id):
        sql = """
        SELECT
            make,
            model,
            name,
            description
        FROM
            camera
        WHERE
            rowid = ?
        """
        params = (camera_id, )
        cur = self.db.cursor()

        self.log.debug(f'Executing: \n{sql}')
        self.log.debug(f'  with params: {params}')

        cur.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return None
        return {
            'make':        row[0],
            'model':       row[1],
            'name':        row[2],
            'description': row[3],
        }

    def get_files(self):
        sql = "SELECT filename_new FROM media"
        cur = self.db.cursor()

        self.log.debug(f'Executing: \n{sql}')

        cur.execute(sql)
        for row in cur.fetchall():
            yield row[0]

    def get_file_details(self, filename):
        sql = """
        SELECT
            filename_original,
            filename_new,
            sha256sum,
            size,
            create_date,
            camera_id
        FROM
            media
        WHERE
            filename_new = ?
        """
        params = (filename,)
        cur = self.db.cursor()

        self.log.debug(f'Executing: \n{sql}')
        self.log.debug(f'  with params: {params}')

        cur.execute(sql, params)
        row = cur.fetchone()
        camera = self.get_camera_from_id(row[5])
        return {
            'filename_original':  row[0],
            'filename_new':       row[1],
            'sha256sum':          row[2],
            'size':               row[3],
            'create_date':        row[4],
            'camera_id':          row[5],
            'camera_make':        camera['make'],
            'camera_model':       camera['model'],
            'camera_short':       camera['name'],
            'camera_description': camera['description'],
        }

    def file_exists_in_db(self, media):
        sql = "SELECT count(*) FROM media WHERE sha256sum = ?"
        params = (media.sha256sum,)
        cur = self.db.cursor()

        self.log.debug(f'Executing: \n{sql}')
        self.log.debug(f'  with params: {params}')

        cur.execute(sql, params)
        return cur.fetchone()[0] != 0

    def insert_image_into_db(self, media):
        oldname     = os.path.split(media.filename)[1]
        newname     = media.newname
        checksum    = media.sha256sum
        size        = media.size
        create_date = media.date
        camera_id   = self.camera_id_from_model(media.model)
        if self.file_exists_in_db(media):
            return False
        sql = """
        INSERT INTO
            media (
                filename_original,
                filename_new,
                sha256sum,
                size,
                create_date,
                camera_id
            ) values (?, ?, ?, ?, ?, ?)
        """
        params = (
            oldname,
            newname,
            checksum,
            size,
            create_date,
            camera_id
        )
        cur = self.db.cursor()

        self.log.debug(f'Executing: \n{sql}')
        self.log.debug(f'  with params: {params}')

        cur.execute(sql, params)
        return True


class Media:
    def __init__(self, **kwargs):
        self.filename = kwargs.get("filename")
        if not self.filename:
            raise Exception(f"No filename provided.")
        if not os.path.exists(self.filename):
            raise FileNotFoundError(f"File {self.filename} does not exist.")

        self.db = kwargs.get("db")
        
        self.dateformat = "%Y:%m:%d %H:%M:%S"
        
        self.__size      = None
        self.__date      = None
        self.__exif      = None
        self.__make      = None
        self.__model     = None
        self.__sha256sum = None
        self.__md5sum    = None
        self.__newname   = None

        self.exts = {
            "image": (
                "jpg",
                "jpeg",
                "tif",
                "tiff",
                "raw",
                "png",
                "bmp",
            ),
            "video": (
                "mp4",
            ),
            "future": (
                "mp3",
                "wav",
                "flac",
                "mkv",
                "avi",
            )
        }

    def _get_exif_value(self, key):
        return self.exif[key]

    def recognized(self):
        return self.is_image() or self.is_video()

    def is_video(self):
        return self.ext in self.exts['video']

    def is_image(self):
        return self.ext in self.exts['image']

    @property
    def sha256sum(self):
        if not self.__sha256sum:
            sha = hashlib.sha256()
            BUFSIZE = 4096
            with open(self.filename, 'rb') as f_in:
                while True:
                    data = f_in.read(BUFSIZE)
                    if not data:
                        break
                    sha.update(data)
            self.__sha256sum = sha.hexdigest()
        return self.__sha256sum

    @property
    def ext(self):
        return os.path.splitext(self.filename)[1][1:].lower()

    @property
    def make(self):
        if not self.__make:
            self.__make = self._get_exif_value("EXIF:Make")
        return self.__make
    
    @property
    def model(self):
        if not self.__model:
            self.__model = self._get_exif_value("EXIF:Model")
        return self.__model

    @property
    def exif(self):
        if not self.__exif:
            # self.__exif = Image.open(self.filename).getexif()
            self.__exif = {}
            with ExifToolHelper() as et:
                for data in et.get_metadata(self.filename):
                    for key, value in data.items():
                        if self.__exif.get(key):
                            raise Exception(f"Key {key} exists twice in {self.filename} metadata")
                        self.__exif[key] = value
        return self.__exif
    
    @property
    def size(self):
        if not self.__size:
            self.__size = os.stat(self.filename).st_size
        return self.__size

    @property
    def date(self):
        if not self.__date:
            self.__date = datetime.datetime.strptime(self._get_exif_value("EXIF:CreateDate"), self.dateformat)
        return self.__date

    @property
    def newname(self):
        if not self.__newname:
            _name = self.db._get_camera_name_from_model(self.model)
            _date = self.date
            _datedir = _date.strftime("%Y-%m")
            _newname = _date.strftime("%Y-%m-%d %H.%M.%S") + "." + _name + "." + self.ext

            self.__newname = os.path.join(
                _name,
                _datedir,
                _newname
            )
        return self.__newname


class ImageSort:
    def __init__(self, args):
        self.args = args

        self.loglevel = logging.DEBUG if self.args.debug else logging.INFO
        self.logformat = "%(asctime)s - %(name)s - %(filename)s:%(lineno)-4d - %(levelname)-8s - %(message)s"
        self.log = logging
        self.log.basicConfig(
            format=self.logformat,
            level=self.loglevel
        )
        logging.getLogger("PIL.TiffImagePlugin").setLevel(logging.INFO)

        self.db = DB(log=self.log)

        self.update_threshold = 25

    def verify(self):
        _check_checksums = self.args.checksums
        for _file in self.db.get_files():
            self.log.debug(f"Found file: {_file}")
            details = self.db.get_file_details(_file)
            filename = os.path.join(self.db.get_destination(), details['filename_new'])
            if not os.path.exists(filename):
                self.db.delete_file(details['filename_new'])
            if _check_checksums and details['sha256sum'] != self.sha256sum(filename):
                self.checksum_dont_match('filename_new')

    def _ask_yesno_question(self, message, default=None):
        if default:
            default = default.lower()
            if default == "yes":
                default = "y"
            if default == "no":
                default = "n"
        else:
            default = "y"

        if default not in ("y", "yes", "n", "no"):
            raise ValueError("Invalid default value.  Must be one of Y, Yes, N, No")

        prompt = f"{message}"
        if default == "y":
            prompt += " [Y]/N"
        else:
            prompt += " Y/[N]"
        prompt += ": "
        while True:
            choice = input(prompt).lower()
            if not choice:
                return choice
            if choice.lower() not in ("y", "yes"):
                continue

    def _ask_word_question(self, message, default=None):
        prompt = f"{message}"
        if default:
            prompt += f" [{default}]"
        prompt += ": "

        choice = input(f"{prompt}")

        if choice.lower() in (""):
            choice = default
        return choice

    def _new_camera(self, model):
        print()
        print(f"Found new camera: {model}")
        while True:
            message = f"Familiar camera name (will be used for directory names, etc)"
            _name = self._ask_word_question(message, model)
            
            message = "Long description of camera?"
            _desc = self._ask_word_question(message, model)

            choice = input(f"{_name}: [{_desc}] - Correct? [Y]/n: ")
            print()
            if choice.lower().startswith('y') or not choice:
                return _name, _desc
    
    def _copy(self, image):
        oldname = image.filename
        newname = os.path.join(self.db.get_destination(), image.newname)
        if os.path.exists(newname):
            return False
        _dir = os.path.split(newname)[0]
        self.log.info(f"Copying: {oldname} => {newname}")
        if not os.path.exists(_dir):
            self.log.debug(f"Directory {_dir} already exists.")
            os.makedirs(_dir)
        shutil.copy2(oldname, newname)

    def _handle_file(self, image):
        inserted = self.db.insert_image_into_db(image)
        self.log.debug(f"Inserted: {inserted}")
        copied   = self._copy(image)
        self.log.debug(f"Copied: {copied}")
        return copied or inserted

    def sha256sum(self, filename):
        sha = hashlib.sha256()
        BUFSIZE = 4096
        with open(filename, 'rb') as f_in:
            while True:
                data = f_in.read(BUFSIZE)
                if not data:
                    break
                sha.update(data)
        return sha.hexdigest()

    def sort(self):
        count = 0
        for root, _, files in os.walk(self.args.directory):
            for _file in files:
                filename = os.path.join(root, _file)
                self.log.info(f"File: {filename}")
                _image = Media(filename=filename, db=self.db)

                if not _image.recognized():
                    continue

                if not self.db.camera_model_exists(_image.model):
                    _name, _desc = self._new_camera(_image.model)
                    self.db.add_camera(
                        make=_image.make,
                        model=_image.model,
                        name=_name,
                        desc=_desc
                    )
                updated = self._handle_file(_image)
                if updated:
                    count += 1
                    if count % self.update_threshold == 0:
                        self.db.db.commit()
        self.db.db.commit()


def scandir(args):
    sort = ImageSort(args)
    sort.sort()

def verify(args):
    sort = ImageSort(args)
    sort.verify()


def main():
    # path = os.path.join(os.path.expanduser('~'), 'imagesort-py', 'images', 'sx740', 'MVI_3434.MP4')
    # vid = MediaVideo()
    # vid2 = vid.testmetadata(path)
    # sys.exit()

    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    
    sub_scandir = subs.add_parser(
        "scan-directory",
        help="Scan directory for new files."
    )
    sub_scandir.add_argument(
        "directory",
        help="Directory to scan.  (Default is current directory)"
    )
    sub_scandir.add_argument(
        "--debug",
        help="Debug logging.",
        action="store_true"
    )
    sub_scandir.set_defaults(func=scandir)

    sub_verify = subs.add_parser(
        "verify",
        help="Verify stored images"
    )
    sub_verify.add_argument(
        "-c",
        "--checksums",
        help="Verify stored checksums (Can add considerable time to verification process.)",
        action="store_true"
    )
    sub_verify.add_argument(
        "--debug",
        help="Debug logging.",
        action="store_true"
    )
    sub_verify.set_defaults(func=verify)

    args = parser.parse_args(sys.argv[1:])
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
