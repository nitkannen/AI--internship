# AI--internship
Contains adder functions and code 



* The script Recommender System.py is used as a recommendation system in the Go Gaga App, both iOS and Android.
* The script essentially calculates the interest match score or the compaitibility score between 2 individuals.
* The initial XML code is the interest levels and questions used in the app UI, using these interests of the user and his/ her preferences of aga and gender, this script
calculates a compatibility score between the user and every other profile.
* The top scores from this compatibility calcuations are recommended to the user.
* The talking point of the script is the fact that the script does a Content Based recommendation and a Collabarative filtering for the same user. With different threshold scores for these two different types of filtering, profiles are suggested to the user.
* Essentially, the content based aspect of the script suggests a prospect whose interests dictionary has a maximum overlap with that of the user.
* The collabarative aspect involves some level of ingenuity from our side. The script takes not of the profiles the user has liked or connected with. With this list the script gives a weight score between 0 and 1 to each of these liked profiles. 1 being a succesful connect and 0 being an immediate reject. Most profiles that connected get a score of 1. Rejected profiles get a weight of : 1 - No.of rejects/(total number of requests sent). This ensures that the profiles similar to those that have rejected the user aren't  shown to the user frequently. 
* With these weighted interest dictionaries, we calculate a weighted sum of the interests of all liked/connected profiles and call it "taste preference" of the user.

Taste Preference  = (*Sigma* alphai X interesti) / N
                            - where alpha is the weight of i-th profile and N is the total number of likes by user.
                            
* We use the Cosine similarity heuristic for all computations.

<img src = "https://images.deepai.org/glossary-terms/cosine-similarity-4063640.jpg">

* We use filters in the script to remove fake profiles out of the picture using various methods
