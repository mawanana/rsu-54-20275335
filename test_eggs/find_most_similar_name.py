from difflib import SequenceMatcher

def most_matching_name(names_list, string):
    # Initialize variables to keep track of the most matching name and its similarity score
    most_matching_name = None
    max_similarity = 0

    # Iterate through the names list
    for name in names_list:
        # Calculate similarity score between the name and the given string
        similarity = SequenceMatcher(None, name.lower(), string.lower()).ratio()

        # Update most matching name and its similarity score if the current name has higher similarity
        if similarity > max_similarity:
            most_matching_name = name
            max_similarity = similarity

    return most_matching_name

# List of names
names_list = ['Graeme Pollock', 'Jean-Paul Duminy', 'AB de Villiers', 'Justin Kemp', 'Mark Boucher', 'Shaun Pollock',
              'Albie Morkel', 'Vernon Philander', 'Johan van der Wath', 'Morne Morkel', 'Makhaya Pollock']

# Given string
string = "G Pollock"

# Get the most matching name
most_matching = most_matching_name(names_list, string)

# Print the result
print("Most matching name:", most_matching)
