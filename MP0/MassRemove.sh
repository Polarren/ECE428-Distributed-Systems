grp="39"
netid="yuhangr2"
gitUsername="yuhangr2"
gitPassword="3381008RYhHH"
gitRepoLink="gitlab.engr.illinois.edu/yuhangr2/ece428_mp1_yuhangr2"
for i in {1..10}
do
    echo "*******************************************************************************************"
    echo "***********************************Removing is Server $i **********************************"
    ssh $netid@sp21-cs425-g`printf $grp`-`printf %02d $i`.cs.illinois.edu "rm -rf ece428_mp1_yuhangr2"
done