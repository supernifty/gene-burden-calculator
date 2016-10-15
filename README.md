
# Exac Burden Calculator

# Running
```
pip install -r requirements.txt
python app.py
```

# Docker
Docker image is currently untested.
```
docker build -t exac-burden:latest .
docker run -d -p 5000:5000 exac-burden:latest
```

# Building the db
```
python build.py --db exac.db --exac data/chr*.txt --length data/gene.prot.len.txt
```
