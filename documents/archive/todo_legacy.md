# on zone mappping we shoud add other formula to addapt the the bounderies of casa by aplying a rotation of all reactangle 
# second adaptation is to respoct the realistic aspet of the real trajectoir and boulvar of casa-blanca 
    ->searching a casa trajectoir that contain all the real gps location of the bv and arroundiseement (on github or other source)
    -

    Choose an Algorithm
Since your goal is to respect "logic boundaries," you should use a Hidden Markov Model (HMM). This is the most common approach because it looks at two things:

Measurement Probability: How close is the GPS point to a road?

Transition Probability: Does the move from Point A to Point B make sense? (e.g., a taxi can't teleport across a block or drive through a wall).

