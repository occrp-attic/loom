import warnings

# shut up useless SA warning:
warnings.filterwarnings(
    'ignore', 'Unicode type received non-unicode bind param value.')
warnings.filterwarnings(
    'ignore', 'At least one scoped session is already present.')
