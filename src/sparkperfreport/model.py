class SparkPerfTestingResults:
    def __init__(self, data):
        self.training_metrics = []
        self.training_time = []
        self.test_metric = []
        self.test_time = []
        only_time = False
        for i in data:
            if only_time:
                self.test_time.append(i.get('time'))
                continue
            if i.get('trainingMetric'):
                self.training_metrics.append(i.get('trainingMetric'))
            if i.get('trainingTime'):
                self.training_time.append(i.get('trainingTime'))
            if i.get('testMetric'):
                self.test_metric.append(i.get('testMetric'))
            if i.get('testTime'):
                self.test_time.append(i.get('testTime'))
            if not self.test_time:
                only_time = True
                self.test_time.append(i.get('time'))
