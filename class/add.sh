clear
echo "......sum of N natural numbers in shell scripting......"
echo -n "Enter the n'th number value: "
read digit
t=1
total=0
while test $t -le $digit
do 
total=`expr $total + $t`
t=`expr $t + 1`
done
echo "sum of $digit: $total"
