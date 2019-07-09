## Set Up test

### clone repository
```bash
git clone https://github.com/Mabloq/setup-test.git
cd setup-test
```

### Create and activate conda evn / virtual env
```bash
conda create -n myenv python=3
source activate myenv
```
### run setup.py
```bash
python setup.py install
```

### run etl
```bash
python etl.py
```

### run test analytical queries
```bash
pyhton analytical_queries.py
```