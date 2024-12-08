for job in `jobs -p`
do
    kill $job
done
