Hadoop PageRank Implmentation
======
## Editor: Yun-Chen Lo
## Input
> <page><title>America</title>...<text ...>America is ...[[Europe]]...</text>...</page> <page><title>Europe</title>...<text ...> ...[[Asia]]...</text>...</page>
> <page><title>Taiwan</title>...<text ...>...officially ..[[Taipei]]..[[HsinChu]]..</text>...</page>
## Job Description
### Job1: Build Graph(Parse)
> 1. Mapper:
    > - Get the title and send title meta data first to all reducers 
    > <“\space” + title, “null”>
    > - Get link and send title link pair
    > <title, link>
> 2. Partitioner:
    > - Send Title meta data to all the reducers
    > - Randomly divide link to one of the reducers
> 3. Reducer:
    > - Receive the title meta data and add it into hashset as lookup table
    > - Receive the title link pair and put it into desired pattern.(i.e. title, N, initial pagerank, outlinks).
### Job2: Calculate PageRank
> 1. Mapper:
    > - Parse the data from previous step & send whole line to the reducers(i.e. title, N, initial pagerank, outlinks).
    > - Send link with its
distribution of the original page rank)
    > <link, input shared weights>
    > - Send dangling node 
    > <"\tab", dangling node>
> 2. Partitioner:
    > - Send the meta data title to all reducers
    > - Divide the original title link to one of the reducer
> 3. Reducer:
    > - Aggregate the dangling node distribution sum(Dangling Node)/N, save it
to hash map
    > - Aggregate the predecessor distribution to current link, and save it to hashmap
    > - Get the original key value pair and count the next page rank from provided equation.

### Job3: Sort PageRank
> 1. Mapper:
    > - Parse the output of previous phase and save it into customized class
SortPair(title, pr)
> 2. Partitioner:
    > - ignore because don't know the distribution of the weights
> 3. Reducer:
    > - output the sorted title pr