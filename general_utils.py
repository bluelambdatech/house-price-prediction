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

    def write_yaml(data, path):
        with open(path, "w") as file:
            yaml.dump(data, file)

    # # Example usage:
    # data = {"key1": "value1", "key2": "value2"}
    # yaml = write_yaml(data, "output.yaml")


if __name__ == "__main__":
    print(load_yaml("param.yaml")["url"])
    data = {"key1": "value1", "key2": "value2"}
    print(write_yaml(data, "output_yaml"))
