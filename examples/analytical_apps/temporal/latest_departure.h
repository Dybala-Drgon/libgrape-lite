#ifndef LATEST_DEPARTURE_H
#define LATEST_DEPARTURE_H
#include <grape/grape.h>

#include "temporal/latest_departure_context.h"
namespace grape {
template <typename FRAG_T>
class Temporal_LD : public AutoAppBase<FRAG_T, LDContext<FRAG_T>> {
 public:
  INSTALL_AUTO_WORKER(Temporal_LD<FRAG_T>, LDContext<FRAG_T>, FRAG_T);
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

 private:
  void ComputeLD(const fragment_t& frag, context_t& ctx,
                 std::priority_queue<std::pair<int64_t, vertex_t>>& que) {
    auto inner_vertices = frag.InnerVertices();

    VertexArray<typename FRAG_T::inner_vertices_t, bool> modified(
        inner_vertices, false);
    while (!que.empty()) {
      vertex_t u;
      int64_t departure_time;
      u = que.top().second;
      departure_time = que.top().first;
      que.pop();
      ctx.partial_result.Reset(u);
      if (modified[u])
        continue;

      modified[u] = true;
      auto edges = frag.GetIncomingAdjList(u);
      for (auto& edge : edges) {
        auto tmp = edge.get_data();
        // TODO: 修改边的结构
        auto arrival_time = tmp + 1;
        if (arrival_time > departure_time)
          continue;
        if (tmp < ctx.start_time && ctx.start_time <= ctx.end_time)
          continue;

        vertex_t v = edge.get_neighbor();
        int64_t distv = ctx.partial_result[v];
        if (distv < tmp) {
          ctx.partial_result.SetValue(v, tmp);
          if (frag.IsInnerVertex(v)) {
            que.emplace(tmp, v);
          }
        }
      }
    }
  }

 public:
  void PEval(const fragment_t& frag, context_t& ctx) {
    std::cout << "执行PEval" << std::endl;
    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);
    std::priority_queue<std::pair<int64_t, vertex_t>> que;
    if (native_source) {
      ctx.partial_result.SetValue(source, ctx.end_time);
      que.emplace(ctx.end_time, source);
    }
    ComputeLD(frag, ctx, que);
  }
  void IncEval(const fragment_t& frag, context_t& ctx) {
    std::cout << "执行Inc" << std::endl;
    auto inner_vertices = frag.InnerVertices();
    std::priority_queue<std::pair<int64_t, vertex_t>> que;
    for (auto& v : inner_vertices) {
      if (ctx.partial_result.IsUpdated(v)) {
        que.emplace(ctx.partial_result.GetValue(v), v);
      }
    }
    ComputeLD(frag, ctx, que);
  }
};
}  // namespace grape

#endif