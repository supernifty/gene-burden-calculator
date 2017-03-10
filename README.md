
# Exac Burden Calculator

# Running
```
virtualenv -p python3 burden-env
source burden-env/bin/activate
pip install -r requirements.txt
python app.py
```

# Running from Docker

Download and extract the asset bundle (approx 58GB) from https://swift.rc.nectar.org.au:8888/v1/AUTH_24376b6176a5466b9f20bee02ee1f182/burgene-assets-170310.tgz

```
docker run -d -p 5000:5000 -v /mnt/work/burgene-assets/:/app/assets/ burgene:latest
```

Replace "/mnt/work/burgene-assets" with the location of your extracted asset bundle.

Browse to http://127.0.0.1:5000/


# Building the app and assets
You only need to continue reading if you are interested in building the app.

## Building the Docker image

```
docker build -t burgene:latest .
```

## Building the assets

```
tar cvfz burgene-assets-170310.tgz burgene-assets/
swift upload -S 419430400 --object-threads 5 --segment-threads 5 burgene-assets-170310.tgz
```

## Building the db
```
python build.py --db exac.db --exac data/chr*.txt --length data/gene.prot.len.txt
```
