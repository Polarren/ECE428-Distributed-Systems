grp="39"
netid="yuhangr2"
gitUsername="yuhangr2"
gitPassword="3381008RYhPP"
gitRepoLink="gitlab.engr.illinois.edu/yuhangr2/ece428_mp1_yuhangr2"
for i in {1..10}
do
    echo "*******************************************************************************************"
    echo "***********************************Cloning is Server $i ***********************************"
    ssh $netid@sp21-cs425-g`printf $grp`-`printf %02d $i`.cs.illinois.edu "git clone https://$gitUsername:$gitPassword@$gitRepoLink; exit"
done