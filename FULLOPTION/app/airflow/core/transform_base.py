class TransformBase:
    def __init__(self, dfs:dict):
        self.dfs = dfs

    @staticmethod
    def excute(transformation, dfs):
        transform = transformation(dfs)
        return transform.transform()

    def transform(self):
        raise NotImplementedError()
