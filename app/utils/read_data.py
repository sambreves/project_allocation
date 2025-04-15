import chardet

def detect_enconding(url):
    try:
        #Detectar enconding
        with open(url, 'rb') as f:
            result = chardet.detect(f.read())
        encoding = result['encoding']
        print(encoding)
    except TypeError as error:
        print('Error: ', error)
        
