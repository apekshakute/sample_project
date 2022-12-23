#ip=int(input("input  :"))
ip="128"
input_1=ip

found=False
while(found==False):
    str1=str(input_1)[::-1]
    if str1 == str(input_1):
        found=True
        print(f"palindrome: {str1}")
        next_palindrome=str1
        break
    else:
        input_1 += 1

input_1=ip
found=False
while(found==False):
    str1=str(input_1)[::-1]
    if str1 == str(input_1):
        found=True
        print(f"palindrome: {str1}")
        prev_palindrome=str1
        break
    else:
        input_1 = input_1- 1
near_palindrome=""  
print(int(next_palindrome) - int(ip))
print(int(ip) - int(prev_palindrome))
if (int(next_palindrome) - int(ip)) > (int(ip) - int(prev_palindrome)):
    near_palindrome=prev_palindrome
else:
    near_palindrome=next_palindrome
print(f"near palindrome:{near_palindrome}")
