
IGNORE_TESTS = [
    'pic',
    'chi-sq-mat',
    'kmeans',
    'chi-sq-gof',
    'gmm',
]

MLLIB_TESTS = [
    'als',
    'lda-1',
    'lda-2',
    'svd',
    'pca',
    'summary-statistics',
    'block-matrix-mult',
    'pearson',
    'spearman',
    'chi-sq-feature',
    'word2vec',
    'fp-growth',
    'prefix-span',
    'glm-regression',
    'glm-classification-1',
    'glm-classification-2',
]

WRITER_SETTINGS = {
    'spark': {
        'worksheet_name': 'core',
        'prefix': 'C'
    },
    'decision-tree': {
        'prefix': 'DTR'
    },
    'mllib': {
        'prefix': 'ML',
        'keys': MLLIB_TESTS,
        'first_column_title': 'Назва тесту',
    }
}

STATISTICS_PARAMS = ['stdev', 'mean', 'median']
