import yaml


def load_yaml(path):
    with open(path,"r") as p:
        file = yaml.safe_load(p)

    return file


if __name__ == "__main__":
    print(load_yaml("param.yaml")["url"])

