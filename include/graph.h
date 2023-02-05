#ifndef GRAPH_H_
#define GRAPH_H_

#include <string>
#include <map>
#include <vector>
#include <time.h>


enum Status {
    kStart=0,
    kDoing=1,
    kEnd=2,
};

struct Event {
    int id;
    std::string content;
    struct tm createdAt;
};

struct Work {
    int id;
    std::string  content;
    std::vector<std::string> related_people;
    Status status;
    int priority;
    std::vector<Event> events;
    struct tm updatedAt;
};

struct Relation {
    int id;
    int w1;
    int w2;
    std::string description;
};

struct Graph {
    int id;
    std::map<std::string, Work> works;
    std::map<std::string, Relation> relations;
    std::string name;
};

#endif