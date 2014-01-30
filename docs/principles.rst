Design principles
=================

This section explains the philosophy behind Bosun development and how we can
fit a model to it.

********
Concepts
********

Bosun tasks acts over data sources and sinks.
Tasks can be divided in four categories: Prepare, Compile, Run and Archive.
Data sources and sinks can be Experiments, Code, Artifacts and Storage.

.. image:: https://docs.google.com/drawings/d/1OuNhnngb34NPufprCS1nGV7CRoqFBb25vdamvYDUX4o/pub?w=483&amp;h=356

Execution
---------

In a very high level executing a model can be divided in four parts:

 * Prepare

   This step can include data manipulation (download, munging, copying)
   and directory creation.

 * Compile

   Code checkout from repositories, instrumentation and compilation are
   good fits for this step.

 * Run

   How to run the model: batch system submission, consistency checks,
   status and automatic restarts.

 * Archive

   

Sources and sinks
-----------------

 * Experiment

 * Code

 * Artifacts

 * Storage

*****************************
Small tasks, composable tasks
*****************************

Tasks should be small, and complex tasks should be broken in smaller ones.
