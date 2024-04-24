import difflib

def find_most_matching_name(input_name, players_list):
    # Calculate similarity score for each name in the players_list
    similarity_scores = [(name, difflib.SequenceMatcher(None, input_name, name).ratio()) for name in players_list]

    # Sort the list of tuples based on similarity score in descending order
    sorted_names = sorted(similarity_scores, key=lambda x: x[1], reverse=True)

    # Return the most matching name
    return sorted_names[0][0]

# def find_most_matching_name(input_name, players_list):
#     # Split the input name into parts
#     input_parts = input_name.split()
#
#     # Initialize variables to store the most matching name and its similarity score
#     most_matching_name = ""
#     max_similarity_score = 0
#
#     # Iterate through each name in the players_list
#     for name in players_list:
#         # Split the name into parts
#         name_parts = name.split()
#
#         # Initialize a variable to store the total similarity score for this name
#         total_similarity_score = 0
#
#         # Iterate through each part of the input name
#         for input_part in input_parts:
#             # Find the similarity score for this part of the input name with each part of the name
#             similarity_scores = [difflib.SequenceMatcher(None, input_part, name_part).ratio() for name_part in name_parts]
#
#             # Get the maximum similarity score for this part
#             max_similarity = max(similarity_scores)
#
#             # Add the maximum similarity score to the total similarity score for this name
#             total_similarity_score += max_similarity
#
#         # Calculate the average similarity score for this name
#         average_similarity_score = total_similarity_score / len(input_parts)
#
#         # Update the most matching name if the average similarity score is greater than the current maximum
#         if average_similarity_score > max_similarity_score:
#             max_similarity_score = average_similarity_score
#             most_matching_name = name
#
#     # Return the most matching name
#     return most_matching_name
