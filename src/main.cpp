#include <time.h>
#include <memory>
#include <iostream>
#include <map>
#include <sstream>
#include <cstdio>
#include <locale.h>
#include <algorithm>
#include <unistd.h>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/env.h"
#include "graph.h"
#include "json11.hpp"
#include "gflags/gflags.h"
#include "util.h"


DEFINE_string(gn, "", "graph name");
DEFINE_int32(gi, 0, "graph id");
DEFINE_string(wc, "", "work content");
DEFINE_int32(wi, 0, "work id");
DEFINE_string(wrp, "", "comma separated work related people");
DEFINE_int32(ws, 0, "work status");
DEFINE_int32(wp, 0, "work priority");
DEFINE_string(ec, "", "event content");
DEFINE_int32(ei, 0, "event id");
DEFINE_int32(of, -1, "offset days from now");
// This is a declaration/definition.
// Global namespace can only have declaration/definition, can't have expressions eg: x=3.
// Because TU(translation unit) executed order is not defined.
DEFINE_string(dd, "", "data storage dir");

const std::string kCreate = "ad";
const std::string kList = "li";
const std::string kDelete = "de";
const std::string kUpdate = "up";

const std::string kGraph = "g";
const std::string kWork = "w";

const std::string kEvent = "e";
const std::string kRelation = "r";

const std::string kSeparator = "-";

int SperatorWidth = getStrWidth(kSeparator.c_str());

class GraphManager {
public:
    GraphManager(leveldb::DB *db) : db_(db) {};

    ~GraphManager() { delete db_; }

    int ListGraph(std::vector<Graph *> *graphs);

    int DeleteGraph(int id);

    int GetGraph(Graph *graph, int gi);

    int SaveGraph(Graph *graph);

    int GenerateGraphCheckpoint(int id);

    int ListGraphCheckpoint(int id);

    int DeleteGraphCheckpoint(int graphID, int checkPointID);

    std::string DumpGraph(Graph *graph);


private:
    leveldb::DB *db_;

    void parseRelations(json11::Json obj, std::map<std::string, Relation> &relations);

    void parseRelation(json11::Json obj, Relation *relation);

    void parseEvents(json11::Json obj, std::vector<Event> *events);

    void parseEvent(json11::Json obj, Event *event);

    void parseWorks(json11::Json obj, std::map<std::string, Work> &works);

    void parseWork(json11::Json obj, Work *work);

    void parseGraph(std::map<std::string, json11::Json> obj, Graph *graph);
};


const std::string kGraphPrefix = "graph-";
const std::string kWorkPrefix = "work-";
const std::string kRelationPrefix = "relation-";


void GraphManager::parseGraph(std::map<std::string, json11::Json> items, Graph *graph) {
  auto item = items.find("id");
  if (item != items.end()) {
    graph->id = item->second.int_value();
  }

  item = items.find("name");
  if (item != items.end()) {
    graph->name = item->second.string_value();
  }

  if (items.find("relations") != items.end()) {
    parseRelations(items["relations"], graph->relations);
  }

  if (items.find("works") != items.end()) {
    parseWorks(items["works"], graph->works);
  }
}

void GraphManager::parseRelations(json11::Json obj, std::map<std::string, Relation> &relations) {
  auto items = obj.object_items();
  for (auto it = items.begin(); it != items.end(); it++) {
    Relation relation;
    parseRelation(it->second, &relation);
    relations[it->first] = relation;
  }
}

void GraphManager::parseRelation(json11::Json obj, Relation *relation) {
  auto items = obj.object_items();
  relation->id = items.find("id")->second.int_value();
  relation->w1 = items.find("w1")->second.int_value();
  relation->w2 = items.find("w2")->second.int_value();
  relation->description = items.find("description")->second.string_value();
}

void GraphManager::parseWorks(json11::Json obj, std::map<std::string, Work> &works) {
  auto items = obj.object_items();
  for (auto it = items.begin(); it != items.end(); it++) {
    Work work;
    parseWork(it->second, &work);
    works[it->first] = work;
  }
}

void GraphManager::parseWork(json11::Json obj, Work *work) {
  auto items = obj.object_items();
  work->id = items.find("id")->second.int_value();
  work->content = items.find("content")->second.string_value();
  auto peopleItems = items.find("related_people");
  if (peopleItems != items.end()) {
    for (auto it = peopleItems->second.array_items().begin(); it != peopleItems->second.array_items().end(); it++) {
      work->related_people.push_back(it->string_value());
    }
  }

  work->status = Status(items.find("status")->second.int_value());
  work->priority = Status(items.find("priority")->second.int_value());

  auto es = items.find("events");
  if (es != items.end()) {
    for (auto it = es->second.array_items().begin(); it != es->second.array_items().end(); it++) {
      Event et;
      auto items = it->object_items();
      et.id = items.find("id")->second.int_value();
      et.content = items.find("content")->second.string_value();
      strptime(items.find("created_at")->second.string_value().c_str(), "%Y-%m-%d %H:%M:%S", &(et.createdAt));
      work->events.push_back(et);
    }
  }
  strptime(items.find("updated_at")->second.string_value().c_str(), "%Y-%m-%d %H:%M:%S", &(work->updatedAt));
}

int GraphManager::ListGraph(std::vector<Graph *> *graphs) {
  auto iterator = db_->NewIterator(leveldb::ReadOptions{});
  iterator->Seek(kGraphPrefix);
  while (iterator->Valid()) {
    json11::Json json = json11::Json();
    std::string err;
    json = json.parse(iterator->value().ToString(), err);
    Graph *graph = new Graph;
    parseGraph(json.object_items(), graph);
    graphs->push_back(graph);
    iterator->Next();
  }
  delete iterator;
}

std::string GraphManager::DumpGraph(Graph *graph) {
  json11::Json::object g{
          {"id",   graph->id},
          {"name", graph->name}};
  json11::Json::object works;
  if (!graph->works.empty()) {
    for (auto &it: graph->works) {
      json11::Json::object work{
              {"id",       it.second.id},
              {"content",  it.second.content},
              {"status",   it.second.status},
              {"priority", it.second.priority},
      };
      char buf[255];
      strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &it.second.updatedAt);
      work["updated_at"] = std::string(buf);
      json11::Json::array related_people;
      for (auto &it1: it.second.related_people) {
        related_people.push_back(it1);
      }
      work["related_people"] = related_people;
      json11::Json::array events;
      for (auto &it1: it.second.events) {
        json11::Json::object event{
                {"id",      it1.id},
                {"content", it1.content}};
        char buf[255];
        formatTime(buf, 255, &it1.createdAt);
        event["created_at"] = std::string(buf);
        events.push_back(event);
      }
      work["events"] = events;
      works[it.first] = work;
    }
  }
  json11::Json::object relations;
  if (!graph->relations.empty()) {
    for (auto &it: graph->relations) {
      json11::Json::object relation{
              {"id",          it.second.id},
              {"w1",          it.second.w1},
              {"w2",          it.second.w2},
              {"description", it.second.description}};
      relations[it.first] = relation;
    }
  }
  g["works"] = works;
  g["relations"] = relations;
  json11::Json json = g;
  std::string data = json.dump();
  return data;
}

int GraphManager::SaveGraph(Graph *graph) {
  std::string json_data = DumpGraph(graph);
  std::stringstream s;
  s << kGraphPrefix << graph->id;
  auto status = db_->Put(leveldb::WriteOptions{}, s.str(), json_data);
  if (!status.ok()) {
    std::cerr << "put failed: %v" << std::endl;
    return 1;
  }
  return 0;
}

int GraphManager::GetGraph(Graph *g, int gi) {
  std::stringstream k;
  k << kGraphPrefix << gi;
  auto iterator = db_->NewIterator(leveldb::ReadOptions{});
  iterator->Seek(kGraphPrefix);
  while (iterator->Valid()) {
    if (iterator->key() == k.str()) {
      json11::Json json = json11::Json();
      std::string err;
      json = json.parse(iterator->value().ToString(), err);
      parseGraph(json.object_items(), g);
      delete iterator;
      return 0;
    }
    iterator->Next();
  }
  delete iterator;
  return -1;
}

int GraphManager::DeleteGraph(int id) {
  std::stringstream s;
  s << kGraphPrefix << id;
  auto status = db_->Delete(leveldb::WriteOptions{}, s.str());
  if (!status.ok()) {
    return -1;
  } else {
    return 0;
  }
}

void CreateGraph(GraphManager *gm, std::string gn) {
  std::vector<Graph *> graphs;
  gm->ListGraph(&graphs);
  int max = 0;
  for (auto it = graphs.begin(); it != graphs.end(); it++) {
    if (max < (*it)->id) {
      max = (*it)->id;
    }
    delete *it;
  }
  Graph new_graph;
  new_graph.id = max + 1;
  new_graph.name = gn;
  int ret = gm->SaveGraph(&new_graph);
  if (ret == 0) {
    std::cout << "create graph success" << std::endl;
  } else {
    std::cout << "create graph failed" << std::endl;
  }
}

void ListGraph(GraphManager *gm) {
  std::vector<Graph *> graphs;
  gm->ListGraph(&graphs);

  std::printf("%-10s %-30s\n", "id", "graph_name");
  for (auto it = graphs.begin(); it != graphs.end(); it++) {
    std::printf("%-10d %-30s\n", (*it)->id, (*it)->name.c_str());
    delete *it;
  }
}

void DeleteGraph(GraphManager *gm, int id) {
  if (gm->DeleteGraph(id) != 0) {
    std::cout << "delete graph failed!" << std::endl;
  } else {
    std::cout << "delete graph success!" << std::endl;
  }
}


void CreateWork(GraphManager *gm, int gi, std::string wc, Status ws, int wp, std::string wrp) {
  if (wc.empty()) {
    std::cerr << "work content is empty" << std::endl;
    return;
  }
  Graph g;
  int ret = gm->GetGraph(&g, gi);
  if (ret != 0) {
    std::cerr << "get graph failed: %v" << std::endl;
    return;
  }
  int max_wd = 0;
  for (auto it = g.works.begin(); it != g.works.end(); it++) {
    if (it->second.id > max_wd) {
      max_wd = it->second.id;
    }
  }
  Work new_work = Work{};
  new_work.id = max_wd + 1;
  new_work.content = wc;
  new_work.status = ws;
  new_work.priority = wp;
  time_t t = time(NULL);
  struct tm *tm_local = localtime(&t);
  new_work.updatedAt = *tm_local;

  while (!wrp.empty()) {
    int idx = wrp.find(',');
    if (idx != std::string::npos) {
      new_work.related_people.push_back(wrp.substr(0, idx));
      wrp = wrp.substr(idx + 1, wrp.size() - idx + 1);
    } else {
      new_work.related_people.push_back(wrp);
      break;
    }
  }
  std::stringstream k;
  k << kWorkPrefix << new_work.id;
  g.works[k.str()] = new_work;
  if (gm->SaveGraph(&g) == 0) {
    std::cout << "create work success!" << std::endl;
  } else {
    std::cout << "create work failed!" << std::endl;
  }
}

void UpdateWork(GraphManager *gm, int gi, int wi, std::string wc, Status ws, int wp, std::string wrp) {
  Graph g;
  int ret = gm->GetGraph(&g, gi);
  if (ret != 0) {
    std::cerr << "get graph failed: %v" << std::endl;
    return;
  }
  Work *w;
  for (auto &it: g.works) {
    if (it.second.id == wi) {
      w = &(it.second);
    }
  }
  if (w) {
    if (!wc.empty()) {
      w->content = wc;
    }
    if (ws != kStart) {
      w->status = ws;
    }
    if (wp != 0) {
      w->priority = wp;
    }
    time_t t = time(NULL);
    struct tm *tm_local = localtime(&t);
    w->updatedAt = *tm_local;
  } else {
    return;
  }
  while (!wrp.empty()) {
    w->related_people.clear();
    int idx = wrp.find(',');
    if (idx != std::string::npos) {
      w->related_people.push_back(wrp.substr(0, idx));
      wrp = wrp.substr(idx + 1, wrp.size() - idx + 1);
    } else {
      w->related_people.push_back(wrp);
      break;
    }
  }
  if (gm->SaveGraph(&g) == 0) {
    std::cout << "update work success!" << std::endl;
  } else {
    std::cout << "update work failed!" << std::endl;
  }
}


void DeleteWork(GraphManager *gm, int gi, int wi) {
  Graph g;
  int ret = gm->GetGraph(&g, gi);
  if (ret != 0) {
    std::cerr << "get graph failed: %v" << std::endl;
    return;
  }
  std::stringstream k;
  k << kWorkPrefix << wi;
  auto it = g.works.find(k.str());
  if (it != g.works.end()) {
    g.works.erase(it);
    std::cout << "delete work success" << std::endl;
  } else {
    std::cout << "delete work failed" << std::endl;
  }
  gm->SaveGraph(&g);
}

bool CompareWork(Work &w1, Work &w2) {
  return w1.id < w2.id;
}

void ListWork(GraphManager *gm, int gi) {
  Graph g;
  int ret = gm->GetGraph(&g, gi);
  if (ret != 0) {
    std::cerr << "get graph failed: %v" << std::endl;
    return;
  }
  std::printf("%-10s %-10s %-10s %-30s %-10s %-30s\n", "id", "priority", "status", "created_at", "event", "content");
  std::vector<Work> works;
  for (auto it = g.works.begin(); it != g.works.end(); it++) {
    works.push_back(it->second);
  }
  std::sort(works.begin(), works.end(), CompareWork);
  for (auto it = works.begin(); it != works.end(); it++) {
    char buf[255];
    formatTime(buf, 255, &it->updatedAt);
    std::printf("%-10d %-10d %-10d %-30s %-10d %-30s\n",
                it->id,
                it->priority,
                it->status,
                buf,
                it->events.size(),
                it->content.c_str());
  }
}


void CreateEvent(GraphManager *gm, int gi, int wi, std::string ec) {
  if (ec.empty()) {
    std::cerr << "event content is empty" << std::endl;
    return;
  }
  Graph g;
  int ret = gm->GetGraph(&g, gi);
  if (ret != 0) {
    std::cerr << "get graph failed: %v" << std::endl;
    return;
  }
  std::stringstream s;
  s << kWorkPrefix << wi;
  auto work = g.works.find(s.str());
  if (work != g.works.end()) {
    int max_id = 0;
    for (auto &it: work->second.events) {
      if (it.id > max_id) {
        max_id = it.id;
      }
    }
    time_t t = time(NULL);
    struct tm *tm_local = localtime(&t);
    Event e = Event{max_id + 1, ec, *tm_local};
    work->second.events.push_back(e);
  }
  if (gm->SaveGraph(&g) == 0) {
    std::cout << "creat event success!" << std::endl;
  } else {
    std::cout << "create event failed!" << std::endl;
  }
}

void ListEvent(GraphManager *gm, int gi, int wi) {
  Graph g;
  int ret = gm->GetGraph(&g, gi);
  if (ret != 0) {
    std::cerr << "get graph failed: %v" << std::endl;
    return;
  }
  std::stringstream s;
  s << kWorkPrefix << wi;
  auto work = g.works.find(s.str());
  std::unique_ptr<char[]> buffer;
  std::string sperate_line;
  if (work != g.works.end()) {
    buffer = std::unique_ptr<char[]>(new char[1000 * work->second.events.size()]);
    int max_width = 0;
    int off = 0;
    for (auto &it: work->second.events) {
      char buf[255];
      formatTime(buf, 255, &it.createdAt);
      int l = std::sprintf(buffer.get() + off, "%-10d %-30s %-30s\n", it.id, buf, it.content.c_str());
      int width = getStrWidth(buffer.get() + off);
      off += l;
      if (width > max_width) {
        max_width = width;
      }
    }
    int i = 0;
    while (i < max_width) {
      sperate_line.append("-");
      i += SperatorWidth;
    }
    sperate_line.append("\n");

  }
  std::cout << sperate_line;
  std::printf("%-10s %-30s %-30s\n", "id", "created_at", "content");
  std::cout << sperate_line;
  std::printf(buffer.get());
  std::cout << sperate_line;
  std::cout << "work-id=" << work->second.id << "     " << "work-content=" << work->second.content << std::endl;
  std::cout << sperate_line;
}

void ListEventOffset(GraphManager *gm, int gi, int offset) {
  Graph g;
  int ret = gm->GetGraph(&g, gi);
  if (ret != 0) {
    std::cerr << "get graph failed: %v" << std::endl;
    return;
  }
  time_t now;
  time(&now);
  std::map<std::string, Work> works;
  int events = 0;
  for (auto it = g.works.begin(); it != g.works.end(); it++) {
    for (auto it1 = it->second.events.begin(); it1 != it->second.events.end(); it1++) {
      time_t t1 = mktime(&it1->createdAt);
      double diff = difftime(now, t1);
      if (diff < 3600 * 24 * offset) {
        auto work = works.find(it->first);
        if (work == works.end()) {
          Work w = Work{};
          w.id = it->second.id;
          w.content = it->second.content;
          w.events.push_back(*it1);
          works[it->first] = w;
          events++;
        } else {
          works[it->first].events.push_back(*it1);
          events++;
        }
      }
    }
  }
  int max_width = 0;
  int max_c1 = 10;
  int max_c2 = 10;
  int max_c3 = 10;
  int max_c4 = 20;
  int max_c5 = 15;
  for (auto work = works.begin(); work != works.end(); work++) {
    for (auto &it: work->second.events) {
      char buf[255];
      formatTime(buf, 255, &it.createdAt);

      int c1 = getStrWidth(std::to_string(work->second.id).c_str());
      int c2 = getStrWidth(work->second.content.c_str());
      int c3 = getStrWidth(std::to_string(it.id).c_str());
      int c4 = getStrWidth(buf);
      int c5 = getStrWidth(it.content.c_str());

      if (c1 > max_c1) {
        max_c1 = c1;
      }
      if (c2 > max_c2) {
        max_c2 = c2;
      }
      if (c3 > max_c3) {
        max_c3 = c3;
      }
      if (c4 > max_c4) {
        max_c4 = c4;
      }
      if (c5 > max_c5) {
        max_c5 = c5;
      }
    }
  }
  max_width = max_c1 + max_c2 + max_c3 + max_c4 + max_c5 + 20;
  std::string seprate_line;
  int i = 0;
  while (i < max_width) {
    seprate_line.append("-");
    i += SperatorWidth;
  }

  seprate_line.append("\n");
  std::cout << seprate_line;
  std::string header[] = {"worker-id", "work-content", "event-id", "event-created-at", "event-content"};
  int lengths[] = {max_c1, max_c2, max_c3, max_c4, max_c5};
  for (int i = 0; i < 5; i++) {
    std::printf("%s", header[i].c_str());
    int l = header[i].length();
    while (l < lengths[i] + 5) {
      l++;
      putchar(' ');
    }
  }
  printf("\n");
  std::cout << seprate_line;

  for (auto work = works.begin(); work != works.end(); work++) {
    for (auto &it: work->second.events) {
      char buf[255];
      formatTime(buf, 255, &it.createdAt);
      int c1 = getStrWidth(std::to_string(work->second.id).c_str());
      int c2 = getStrWidth(work->second.content.c_str());
      int c3 = getStrWidth(std::to_string(it.id).c_str());
      int c4 = getStrWidth(buf);
      int c5 = getStrWidth(it.content.c_str());
      std::string work_id = std::to_string(work->second.id);
      std::string event_id = std::to_string(it.id);
      char *row[] = {const_cast<char *>(work_id.c_str()),
                     const_cast<char *>(work->second.content.c_str()),
                     const_cast<char *>(event_id.c_str()),
                     buf,
                     const_cast<char *>(it.content.c_str())
      };
      int lengths[] = {c1, c2, c3, c4, c5};
      int max_lengths[] = {max_c1, max_c2, max_c3, max_c4, max_c5};
      for (int i = 0; i < 5; i++) {
        std::printf("%s", row[i]);
        while (lengths[i] < max_lengths[i] + 5) {
          lengths[i]++;
          putchar(' ');
        }
      }
      printf("\n");
    }
  }
  std::cout << seprate_line << std::endl;
}

void DeleteEvent(GraphManager *gm, int gi, int wi, int ei) {

  Graph g;
  int ret = gm->GetGraph(&g, gi);
  if (ret != 0) {
    std::cerr << "get graph failgzbh-ns-map-na029.gzbh.baidu.comed: %v" << std::endl;
    return;
  }
  std::stringstream s;
  s << kWorkPrefix << wi;
  auto work = g.works.find(s.str());
  if (work != g.works.end()) {
    for (auto it = work->second.events.begin(); it < work->second.events.end(); it++) {
      if (it->id == ei) {
        work->second.events.erase(it);
      }
    }
  }
  if (gm->SaveGraph(&g) == 0) {
    std::cout << "delete event success!" << std::endl;
  } else {
    std::cout << " delete event failed!" << std::endl;
  }
}

int main(int argc, char **argv) {
  setlocale(LC_ALL, "");
  char * home;
  if ((home=getenv("HOME"))== nullptr && FLAGS_dd.empty()) {
    std::cerr << "env $HOME or data storage dir must be set";
    return 1;
  }
  if (FLAGS_dd.empty()) {
    FLAGS_dd = getenv("HOME");
  }
  leveldb::DB *db;
  leveldb::Options option;
  option.create_if_missing = true;
  leveldb::Status status = leveldb::DB::Open(option, FLAGS_dd+"/graph", &db);
  if (!status.ok()) {
    std::cerr << "open db failed: " << status.ToString() << std::endl;
    return 1;
  }
  GraphManager p = GraphManager(db);

  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (argc != 3) {
    std::cout << "wrong arguments" << std::endl;
    return 1;
  }

  std::string action = argv[1];
  std::string resource = argv[2];

  if (action == kCreate) {
    if (resource == kGraph) {
      if (FLAGS_gn.empty()) {
        std::cerr << "emtpy graph name" << std::endl;
        return 1;
      }
      CreateGraph(&p, FLAGS_gn);
    } else if (resource == kWork) {
      CreateWork(&p, FLAGS_gi, FLAGS_wc, static_cast<Status>(FLAGS_ws), FLAGS_wp, FLAGS_wrp);
    } else if (resource == kEvent) {
      CreateEvent(&p, FLAGS_gi, FLAGS_wi, FLAGS_ec);
    } else {
      std::cerr << "unknown resource: " << resource << std::endl;
    }
  } else if (action == kList) {
    if (resource == kGraph) {
      ListGraph(&p);
    } else if (resource == kWork) {
      ListWork(&p, FLAGS_gi);
    } else if (resource == kEvent) {
      if (FLAGS_of < 0) {
        ListEvent(&p, FLAGS_gi, FLAGS_wi);
      } else {
        ListEventOffset(&p, FLAGS_gi, FLAGS_of);
      }
    }
  } else if (action == kDelete) {
    if (resource == kGraph) {
      DeleteGraph(&p, FLAGS_gi);
    } else if (resource == kWork) {
      DeleteWork(&p, FLAGS_gi, FLAGS_wi);
    } else if (resource == kEvent) {
      DeleteEvent(&p, FLAGS_gi, FLAGS_wi, FLAGS_ei);
    }
  } else if (action == kUpdate) {
    if (resource == kWork) {
      UpdateWork(&p, FLAGS_gi, FLAGS_wi, FLAGS_wc, static_cast<Status>(FLAGS_ws), FLAGS_wp, FLAGS_wrp);
    }
  } else {
    std::cerr << "unknown action: " << action << std::endl;
  }
}
