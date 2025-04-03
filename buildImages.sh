version=':v1.0'

cd inserirmongodb
cp -r ../utils .
echo INSERIRMONGODB
docker build -t etlmongo-inserirmongodb${version} .
cd ..
rm -rf inserirmongodb/utils

cd transformardados
cp -r ../utils .
echo TRANSFORMARDADOS
docker build -t etlmongo-transformardados${version} .
cd ..
rm -rf transformardados/utils


cd verificador
cp -r ../utils .
echo VERIFICADOR
docker build -t etlmongo-verificador${version} .
cd ..
rm -rf verificador/utils

cd api
cp -r ../utils .
echo API
docker build -t etlmongo-api${version} .
cd ..
rm -rf api/utils

