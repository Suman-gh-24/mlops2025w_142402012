echo -n "Enter the n'th number value: "
read digit
t=1
sum=0
factorial=1

while test $t -le $digit
do 
  sum=`expr $sum + $t`
  factorial=`expr $factorial \* $t`
  t=`expr $t + 1`
done

echo "Sum of first $digit natural numbers is: $sum"
echo "Factorial of $digit is: $factorial"


