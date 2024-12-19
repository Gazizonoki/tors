# leader stop

rm -r test_dir/cluster_test
docker compose up

#------------------------#

curl http://localhost:8081/debug/leader
docker ps
docker stop ???
curl http://localhost:8082/items -X POST -H "Content-Type: text/plain" -d 'some_data'
docker start ???
curl http://localhost:8082/items/...

#------------------------#
