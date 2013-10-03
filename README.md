Amino
=====

About
-----
Amino empowers analysts/data scientists to experiment with new theories, test them rapidly, discover answers, and progress to the next level of analysis. This iterative, evolving, and engaging process augments and enhances the analyst’s tradecraft; it does not replace it. Amino facilitates a “building blocks” approach to analytics enabling rapid hypothesis testing. It provides analysts the ability to “mix and match” features in ad hoc combinations, essentially playing “what if” scenarios in order to answer more complex questions. This encourages discovery and out-of-the-box thinking, encourages collaboration, and most importantly, builds economical “throw-away” heuristics.

Key Concepts
-------------
Bite-sized vs. Black Box: Amino treats MapReduce analytics as a means for creating “bite-sized” building blocks; atomic, small, aggregate “things” about the data – these are the features. The features are the heart of Amino, they are the ingredients the analyst or data scientist can mix and match to test hypotheses, or learn about what makes a particular entity unique. If the feature does too much, the analysts may not be able to use it as desired. This is a similar concept to object oriented programming – an object should only do one thing.

API for Rapid Feature Creation: Amino's API abstracts away the complexities of MapReduce as well as enforces creation of “bite-sized” features, allowing the developer to focus on analytic logic. The developer should not concern themselves with data parsing tasks, ouput formats or indexing results, they can simply focus on the logic behind the feature. For example, if starting from scratch, a developer would need to find a data source, write a parser, implement the logic of the MapReduce analytic, specify an output, index the output, and present the results to an analyst. Instead, implementing the Amino API will allow them to simplify that to one step, write the analytic logic in one method and inject that into the Amino framework. Ease of creation will saturate Amino with features and allow for rapid feature injection if a new, not-yet-imagined feature is needed.

A Powerful Index:
* Provide fast scans
* Allow highly dimensional scans
* Provide a simple query structure
 
The primary use case for Amino starts with a pattern and ends with a list of entities.  As described, the patterns are a mixture of the features of interest.  To support these goals, the Amino index stores its data as feature vectors, keyed by entities, or entity vectors, keyed by feature values.


License
--------
Amino is licensed under Apache License 2.0


