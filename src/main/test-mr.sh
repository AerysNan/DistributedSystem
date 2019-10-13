echo ""
echo "==> Part I"
go test -run Sequential distributed/mapreduce/...
echo ""
echo "==> Part II"
(sh ./test-wc.sh > /dev/null)
echo ""
echo "==> Part III"
go test -run TestParallel distributed/mapreduce/...
echo ""
echo "==> Part IV"
go test -run Failure distributed/mapreduce/...
echo ""
echo "==> Part V (inverted index)"
(sh ./test-ii.sh > /dev/null)

rm mrtmp.* diff.out
