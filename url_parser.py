from urllib.parse import urlsplit, parse_qs


url = """https://www.mydomain.com/page-name?utm_content=textlink&utm_medium=social&utm_source=twitter&utm_campaign=fallsale"""

split_url = urlsplit(url)
params = parse_qs(split_url.query)

# domain
print(split_url.netloc)

# url path
print(split_url.path)

# utm parameters
print(params['utm_content'][0])
print(params['utm_medium'][0])
print(params['utm_source'][0])
print(params['utm_campaign'][0])
