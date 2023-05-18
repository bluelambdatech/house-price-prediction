import yaml


def load_yaml(path):
    with open(path, "r") as p:
        file = yaml.safe_load(p)

    return file


class Yaml:
    def load_yaml(self, path):
        with open(path, "r") as p:
            file = yaml.safe_load(p)

        return file

    def update_yaml(self, path):
        with open(path, "w") as p:
            file = yaml.safe_load(p)

        return


if __name__ == "__main__":
    print(load_yaml("param.yaml")["url"])
