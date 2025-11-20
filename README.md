[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/0ek2UV58)

Gray area thingies:

- In WRITE we have sentences as 1-indexed while within the sentence, it is 0-indexed. We consider anything lesser than 0 to be 0 and anything greater than the number of words to refer to the last word in the sentence. When we add new text in a sentence, we add a space after the word it was supposed to be added at.

- We have 2 levels of locking for the file. One read-write lock (pthread_rwlock_t) for the entire file and a read-write lock for each sentence of the file.

- For CREATE, DELETE and UNDO, we acquire the file write lock and for READ, WRITE, STREAM and EXEC we acquire the file read lock.

- Then within the sentence, for READ, STREAM and EXEC we acquire the sentence read lock while for WRITE we acquire the sentence write lock.

- We used pthread_rwlock_t (general read-write lock) for our project even though we are aware of the possible writers' starvation since we assumed that there will be many more readers than writers and due to its ease in implementation.
