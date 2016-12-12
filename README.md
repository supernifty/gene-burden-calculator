
# Exac Burden Calculator

# Running
```
virtualenv -p python3 burden-env
source burden-env/bin/activate
pip install -r requirements.txt
python app.py
```

# Docker
Docker image is currently untested.
```
docker build -t burgene:latest .
docker run -d -p 5000:5000 burgene:latest
```

# Building the db
```
python build.py --db exac.db --exac data/chr*.txt --length data/gene.prot.len.txt
```
