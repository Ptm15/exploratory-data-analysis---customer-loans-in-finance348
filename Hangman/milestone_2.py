import random

# Creating a list with 5 fruits 

word_list = ["apple", "orange", "kiwi", "mango", "cherry"]

# using the random method to select a random word from list in my output - assigned to the variable word 

word = (random.choice(word_list))

print(word)

# Asking the user to put a random letter using the input function 

guess = input("Enter a random letter")
print("")

# creating an if statement that checks if the length of the input is equal to 1 and the input is alphabetical

if len(guess) == 1 and guess.isalpha():
    print("Good guess!")

else:
    print("Oops! That is not a valid input")

