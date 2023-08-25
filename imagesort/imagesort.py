import sys
import os
import logging
import yaml


class ImageSort:
    def __init__(self):
        self.settings_file = os.path.splitext(os.path.abspath(__file__))[0] + ".yml"
        self.set_settings()
        self.settings_modified = False

        self.loglevel = logging.INFO
        self.logformat = "%(asctime)s - %(name)s - %(pathname)s:%(lineno)-4d - %(levelname)-8s - %(message)s"
        self.log = logging
        self.log.basicConfig(
            format=self.logformat,
            level=self.loglevel
        )
    
    def save_settings(self, force=False):
        if self.settings_modified or force:
            self.log.info(f"Saving settings: {self.settings_file}")
            open(self.settings_file, 'w').write(yaml.dump(self.settings))

    def get_settings(self):
        if not os.path.exists(self.settings_file):
            self.log.debug(f"Settings file not found: {self.settings_file}. Creating.")
            self.settings_modified = True
            self.settings = {}
            while True:
                destination = input("Directory to store images?: ")
                if not destination:
                    True
                choice = input(f"Destination directory: {destination} [Y]/N?: ")
                if choice.lower().startswith('y'):
                    self.settings['destination'] = destination
                    break
            open(self.settings_file, 'w').write(yaml.dump(self.settings))


def main():
    sort = ImageSort()


if __name__ == "__main__":
    main()
