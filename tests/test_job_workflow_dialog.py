import sys
from types import SimpleNamespace
from unittest.mock import patch

from PySide6.QtCore import QDate, QPointF
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QApplication, QLabel

from src.job_workflow.graph.edge_item import _GraphEdgeItem
from src.job_workflow.graph.builder import build_default_graph
from src.job_workflow.graph.geometry import auto_layout_graph, build_connection_path
from src.settings import AppSettings
from src.workflow import FileEntry, WorkflowJob
from src.job_workflow.dialog import JobWorkflowDialog, _node_visual_state, _planned_job_steps


_app = QApplication.instance() or QApplication(sys.argv)


def _make_job(**kwargs) -> WorkflowJob:
    job = WorkflowJob(
        name="Workflow Job",
        source_mode="files",
        files=[FileEntry(source_path="/tmp/a.mp4")],
        **kwargs,
    )
    return job


def _settings() -> AppSettings:
    return AppSettings()


def _node_id_by_type(graph_view, node_type: str) -> str:
    node_id = graph_view.find_node_id_by_type(node_type)
    assert node_id is not None
    return node_id


def _node_by_type(graph_view, node_type: str):
    node = graph_view.node_item(_node_id_by_type(graph_view, node_type))
    assert node is not None
    return node


class TestJobWorkflowDialog:
    def test_graph_edge_exposes_target_arrow_for_direction(self):
        edge = _GraphEdgeItem(SimpleNamespace(remove_edge=lambda *_args, **_kwargs: None), "source", "target")
        edge.setPath(build_connection_path(QPointF(220, 100), QPointF(520, 260)))

        arrow = edge._target_arrow_polygon()

        assert arrow is not None
        assert arrow.count() == 3
        tip = arrow.at(0)
        assert abs(tip.x() - 520.0) < 0.01
        assert abs(tip.y() - 260.0) < 0.01

    def test_connection_path_prefers_clean_direct_curve_for_forward_connections(self):
        path = build_connection_path(
            QPointF(220, 160),
            QPointF(520, 160),
        )

        early = path.pointAtPercent(0.2)
        late = path.pointAtPercent(0.8)

        assert early.x() > 220.0
        assert late.x() < 520.0

    def test_connection_path_ignores_obstacle_context_and_keeps_simple_curve(self):
        obstacle = QApplication.instance()  # keep Qt initialized for path math in tests
        assert obstacle is not None

        from PySide6.QtCore import QRectF

        path = build_connection_path(
            QPointF(220, 100),
            QPointF(220, 280),
            obstacles=[QRectF(170, 135, 120, 100)],
        )

        early = path.pointAtPercent(0.2)
        late = path.pointAtPercent(0.8)

        assert early.x() > 220.0
        assert late.x() > 220.0

    def test_connection_path_avoids_inner_corridor_for_stacked_nodes(self):
        from PySide6.QtCore import QRectF

        start = QPointF(220, 100)
        end = QPointF(220, 280)

        path = build_connection_path(
            start,
            end,
        )

        blocked = QRectF(196, 120, 48, 140)
        for step in range(41):
            point = path.pointAtPercent(step / 40)
            assert not blocked.contains(point), f"stacked route cuts through node corridor at {point}"

        early_point = path.pointAtPercent(0.2)
        assert early_point.x() >= start.x() + 20.0

    def test_connection_path_keeps_forward_branch_outputs_without_source_loop(self):
        path = build_connection_path(
            QPointF(580, 180),
            QPointF(680, 120),
        )

        samples = [path.pointAtPercent(step / 20) for step in range(11)]
        x_values = [point.x() for point in samples]
        assert x_values[1:7] == sorted(x_values[1:7])

    def test_connection_path_uses_rounded_turns_for_readability(self):
        path = build_connection_path(
            QPointF(220, 100),
            QPointF(520, 260),
        )

        assert path.elementCount() == 4

    def test_editor_sidebar_stays_compact_but_usable(self):
        dlg = JobWorkflowDialog(None, _make_job(convert_enabled=True), allow_edit=True, settings=_settings())

        assert dlg._palette_box.maximumWidth() <= 260
        assert dlg._inspector_box.minimumWidth() >= 320
        assert dlg._inspector_box.maximumWidth() <= 460

    def test_auto_layout_aligns_processing_nodes_with_connected_flow(self):
        dlg = JobWorkflowDialog(None, _make_job(convert_enabled=False), allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        source_top = dlg._graph_view.add_node("source_files", pos=QPointF(80, 100), node_id="source-top")
        source_bottom = dlg._graph_view.add_node("source_folder_scan", pos=QPointF(80, 240), node_id="source-bottom")
        processing_top = dlg._graph_view.add_node("titlecard", pos=QPointF(360, 260), node_id="processing-top")
        processing_bottom = dlg._graph_view.add_node("convert", pos=QPointF(360, 100), node_id="processing-bottom")
        delivery_top = dlg._graph_view.add_node("youtube_upload", pos=QPointF(680, 100), node_id="delivery-top")
        delivery_bottom = dlg._graph_view.add_node("stop", pos=QPointF(680, 240), node_id="delivery-bottom")

        dlg._graph_view.connect_nodes(source_top, processing_bottom)
        dlg._graph_view.connect_nodes(source_bottom, processing_top)
        dlg._graph_view.connect_nodes(processing_bottom, delivery_top)
        dlg._graph_view.connect_nodes(processing_top, delivery_bottom)

        auto_layout_graph(dlg._graph_view)

        top_processing = dlg._graph_view.node_item(processing_bottom)
        bottom_processing = dlg._graph_view.node_item(processing_top)
        assert top_processing is not None
        assert bottom_processing is not None
        assert top_processing.pos().y() < bottom_processing.pos().y()

    def test_condition_branches_use_distinct_connection_tracks(self):
        dlg = JobWorkflowDialog(None, _make_job(convert_enabled=False), allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        validate_id = dlg._graph_view.add_node("validate_surface", pos=QPointF(360, 180), node_id="validate")
        repair_id = dlg._graph_view.add_node("repair", pos=QPointF(680, 120), node_id="repair")
        stop_id = dlg._graph_view.add_node("stop", pos=QPointF(680, 260), node_id="stop")
        dlg._graph_view.connect_nodes(validate_id, repair_id, "repairable")
        dlg._graph_view.connect_nodes(validate_id, stop_id, "irreparable")

        paths = {
            branch: edge.path()
            for _source_id, _target_id, branch, edge in dlg._graph_view._edges
        }

        repair_mid = paths["repairable"].pointAtPercent(0.32)
        stop_mid = paths["irreparable"].pointAtPercent(0.32)
        assert abs(repair_mid.y() - stop_mid.y()) >= 20.0

    def test_graph_edges_can_be_removed_explicitly(self):
        job = _make_job(convert_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        source_id = _node_id_by_type(dlg._graph_view, "source_files")
        convert_id = _node_id_by_type(dlg._graph_view, "convert")

        assert (source_id, convert_id) in dlg._graph_view.edge_pairs()
        dlg._graph_view.remove_edge(source_id, convert_id)

        assert (source_id, convert_id) not in dlg._graph_view.edge_pairs()

    def test_graph_output_hotspot_is_detected_without_prior_selection(self):
        job = _make_job(convert_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        source_id = _node_id_by_type(dlg._graph_view, "source_files")
        source_node = dlg._graph_view.node_item(source_id)
        assert source_node is not None
        output_pos = source_node.output_port_pos()

        detected = dlg._graph_view._node_output_at_scene_pos(output_pos + QPointF(8, 0))

        assert detected == source_id

    def test_validation_output_branch_hotspot_returns_branch_key(self):
        job = _make_job(convert_enabled=False)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        validate_id = dlg._graph_view.add_node("validate_surface")
        validate_node = dlg._graph_view.node_item(validate_id)
        assert validate_node is not None
        output_pos = validate_node.output_port_pos("repairable")

        detected = dlg._graph_view._node_output_branch_at_scene_pos(output_pos + QPointF(8, 0))

        assert detected == (validate_id, "repairable")

    def test_graph_can_merge_multiple_sources_and_place_titlecard_before_merge(self):
        job = WorkflowJob(
            name="Graph Job",
            source_mode="files",
            source_folder="/input",
            file_pattern="*.mp4",
            files=[
                FileEntry(source_path="/tmp/a.mp4"),
                FileEntry(source_path="/tmp/b.mp4"),
            ],
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        files_id = dlg._graph_view.add_node("source_files")
        folder_id = dlg._graph_view.add_node("source_folder_scan")
        title_id = dlg._graph_view.add_node("titlecard")
        merge_id = dlg._graph_view.add_node("merge")
        upload_id = dlg._graph_view.add_node("youtube_upload")
        dlg._graph_view.connect_nodes(files_id, title_id)
        dlg._graph_view.connect_nodes(title_id, merge_id)
        dlg._graph_view.connect_nodes(folder_id, merge_id)
        dlg._graph_view.connect_nodes(merge_id, upload_id)
        dlg._sync_draft_from_graph(refresh_graph=False)

        assert dlg._draft.upload_youtube is True
        assert dlg._draft.title_card_enabled is False
        assert all(entry.merge_group_id == "graph-merge" for entry in dlg._draft.files)
        assert all(entry.title_card_before_merge is True for entry in dlg._draft.files)

    def test_planned_steps_follow_merge_before_convert_graph_order(self):
        job = WorkflowJob(
            name="Graph Job",
            source_mode="files",
            files=[FileEntry(source_path="/tmp/a.mp4", merge_group_id="graph-merge", graph_source_id="source-files-1")],
            convert_enabled=True,
            upload_youtube=True,
            graph_nodes=[
                {"id": "source-files-1", "type": "source_files"},
                {"id": "merge-1", "type": "merge"},
                {"id": "convert-1", "type": "convert"},
                {"id": "upload-1", "type": "youtube_upload"},
            ],
            graph_edges=[
                {"source": "source-files-1", "target": "merge-1"},
                {"source": "merge-1", "target": "convert-1"},
                {"source": "convert-1", "target": "upload-1"},
            ],
        )

        assert _planned_job_steps(job) == ["transfer", "merge", "convert", "youtube_upload"]

    def test_planned_steps_include_repair_node_between_titlecard_and_youtube(self):
        job = WorkflowJob(
            name="Repair Job",
            source_mode="files",
            convert_enabled=False,
            files=[FileEntry(source_path="/tmp/a.mp4", graph_source_id="source-files-1")],
            graph_nodes=[
                {"id": "source-files-1", "type": "source_files"},
                {"id": "repair-1", "type": "repair"},
                {"id": "yt-1", "type": "yt_version"},
            ],
            graph_edges=[
                {"source": "source-files-1", "target": "repair-1"},
                {"source": "repair-1", "target": "yt-1"},
            ],
        )

        assert _planned_job_steps(job) == ["transfer", "repair", "yt_version"]

    def test_planned_steps_include_cleanup_and_stop_nodes(self):
        job = WorkflowJob(
            name="Cleanup Stop Job",
            source_mode="files",
            convert_enabled=False,
            files=[FileEntry(source_path="/tmp/a.mp4", graph_source_id="source-files-1")],
            graph_nodes=[
                {"id": "source-files-1", "type": "source_files"},
                {"id": "cleanup-1", "type": "cleanup"},
                {"id": "stop-1", "type": "stop"},
            ],
            graph_edges=[
                {"source": "source-files-1", "target": "cleanup-1"},
                {"source": "cleanup-1", "target": "stop-1"},
            ],
        )

        assert _planned_job_steps(job) == ["transfer", "cleanup", "stop"]

    def test_apply_persists_graph_nodes_and_edges_to_job(self):
        job = _make_job(convert_enabled=False)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files")
        convert_id = dlg._graph_view.add_node("convert")
        upload_id = dlg._graph_view.add_node("youtube_upload")
        dlg._graph_view.connect_nodes(source_id, convert_id)
        dlg._graph_view.connect_nodes(convert_id, upload_id)

        dlg._apply_and_accept()

        assert {node["type"] for node in job.graph_nodes} == {"source_files", "convert", "youtube_upload"}
        assert len(job.graph_edges) == 2

        reopened = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())
        reopened_types = {node.node_type for node in reopened._graph_view.node_items()}
        assert reopened_types == {"source_files", "convert", "youtube_upload"}
        assert len(reopened._graph_view.edge_pairs()) == 2

    def test_apply_persists_validation_edge_branches_to_job(self):
        job = _make_job(convert_enabled=False)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files", node_id="source-files-1")
        validate_id = dlg._graph_view.add_node("validate_surface", node_id="validate-1")
        repair_id = dlg._graph_view.add_node("repair", node_id="repair-1")
        yt_id = dlg._graph_view.add_node("yt_version", node_id="yt-1")
        dlg._graph_view.connect_nodes(source_id, validate_id)
        dlg._graph_view.connect_nodes(validate_id, repair_id, "repairable")
        dlg._graph_view.connect_nodes(validate_id, yt_id, "ok")

        dlg._apply_and_accept()

        assert {edge.get("branch", "") for edge in job.graph_edges if edge["source"] == "validate-1"} == {"repairable", "ok"}

        reopened = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())
        reopened_edges = reopened._graph_view.graph_edges()
        assert {edge.get("branch", "") for edge in reopened_edges if edge["source"] == "validate-1"} == {"repairable", "ok"}

    def test_apply_persists_graph_node_positions(self):
        job = _make_job(convert_enabled=False)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files", pos=QPointF(120, 160))
        convert_id = dlg._graph_view.add_node("convert", pos=QPointF(410, 215))
        dlg._graph_view.connect_nodes(source_id, convert_id)
        dlg._apply_and_accept()

        stored_nodes = {node["id"]: node for node in job.graph_nodes}
        assert stored_nodes[source_id]["x"] == 120.0
        assert stored_nodes[source_id]["y"] == 160.0
        assert stored_nodes[convert_id]["x"] == 410.0
        assert stored_nodes[convert_id]["y"] == 215.0

    def test_moving_node_updates_draft_graph_without_recursion(self):
        job = _make_job(convert_enabled=False)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        source_node = _node_by_type(dlg._graph_view, "source_files")
        original_pos = source_node.pos()

        source_node.setPos(QPointF(original_pos.x() + 25, original_pos.y() + 30))
        QApplication.processEvents()

        stored_nodes = {node["id"]: node for node in dlg._draft.graph_nodes}
        updated = stored_nodes[source_node.node_id]
        assert updated["x"] == original_pos.x() + 25
        assert updated["y"] == original_pos.y() + 30
        assert isinstance(dlg._draft.graph_edges, list)

    def test_view_api_exposes_nodes_without_reaching_into_internal_map(self):
        job = _make_job(convert_enabled=True, upload_youtube=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        node_types = {node.node_type for node in dlg._graph_view.node_items()}
        convert_id = dlg._graph_view.find_node_id_by_type("convert")

        assert {"source_files", "convert", "youtube_upload"}.issubset(node_types)
        assert convert_id is not None
        assert dlg._graph_view.node_type(convert_id) == "convert"
        assert dlg._graph_view.node_item(convert_id) is not None

    def test_default_graph_builder_creates_expected_linear_pipeline(self):
        class _FakeGraphView:
            def __init__(self):
                self.calls = []
                self._next_id = 0

            def add_node(self, node_type):
                self._next_id += 1
                node_id = f"node-{self._next_id}"
                self.calls.append(("add_node", node_type, node_id))
                return node_id

            def connect_nodes(self, source_id, target_id):
                self.calls.append(("connect_nodes", source_id, target_id))

        draft = WorkflowJob(
            source_mode="files",
            files=[FileEntry(source_path="/tmp/a.mp4", merge_group_id="g1")],
            convert_enabled=True,
            title_card_enabled=True,
            create_youtube_version=True,
            upload_youtube=True,
            upload_kaderblick=True,
        )
        graph_view = _FakeGraphView()

        build_default_graph(graph_view, draft)

        assert graph_view.calls == [
            ("add_node", "source_files", "node-1"),
            ("add_node", "convert", "node-2"),
            ("connect_nodes", "node-1", "node-2"),
            ("add_node", "merge", "node-3"),
            ("connect_nodes", "node-2", "node-3"),
            ("add_node", "titlecard", "node-4"),
            ("connect_nodes", "node-3", "node-4"),
            ("add_node", "yt_version", "node-5"),
            ("connect_nodes", "node-4", "node-5"),
            ("add_node", "youtube_upload", "node-6"),
            ("connect_nodes", "node-5", "node-6"),
            ("add_node", "kaderblick", "node-7"),
            ("connect_nodes", "node-6", "node-7"),
        ]

    def test_palette_creates_node_drag_preview_pixmap(self):
        job = _make_job()
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        pixmap = dlg._node_palette._create_drag_pixmap("source_files")

        assert not pixmap.isNull()
        assert pixmap.width() == dlg._node_palette.NODE_PREVIEW_SIZE.width()
        assert pixmap.height() == dlg._node_palette.NODE_PREVIEW_SIZE.height()

    def test_editor_applies_basic_step_flags(self):
        job = _make_job(convert_enabled=True, upload_youtube=False, upload_kaderblick=False)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._name_edit.setText("Neu")
        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files")
        upload_id = dlg._graph_view.add_node("youtube_upload")
        kb_id = dlg._graph_view.add_node("kaderblick")
        dlg._graph_view.connect_nodes(source_id, upload_id)
        dlg._graph_view.connect_nodes(upload_id, kb_id)
        dlg._apply_and_accept()

        assert dlg.changed is True
        assert job.name == "Neu"
        assert job.convert_enabled is False
        assert job.upload_youtube is True
        assert job.upload_kaderblick is True

    def test_editor_disables_kaderblick_without_youtube(self):
        job = _make_job(upload_youtube=True, upload_kaderblick=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        dlg._graph_view.add_node("source_files")

        assert dlg._draft.upload_youtube is False
        assert dlg._draft.upload_kaderblick is False
        assert dlg._kb_game_id_edit.isEnabled() is False

    def test_editor_disables_output_steps_when_no_output_stack_remains(self):
        job = _make_job(convert_enabled=True, title_card_enabled=True, create_youtube_version=True, upload_youtube=False)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        dlg._graph_view.add_node("source_files")

        assert dlg._draft.convert_enabled is False
        assert dlg._draft.title_card_enabled is False
        assert dlg._draft.create_youtube_version is False
        assert dlg._tc_home_edit.isEnabled() is False
        assert dlg._tc_logo_edit.isEnabled() is False

    def test_editor_keeps_merge_step_from_source_groups(self):
        job = WorkflowJob(
            name="Merge Job",
            source_mode="files",
            files=[
                FileEntry(source_path="/tmp/a.mp4", merge_group_id="g1"),
                FileEntry(source_path="/tmp/b.mp4", merge_group_id="g1"),
            ],
            convert_enabled=True,
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        assert "merge" in _planned_job_steps(dlg._draft)
        assert "Merge ist aktiv" in dlg._merge_label.text()
        assert dlg._file_list_widget is not None

    def test_editor_hint_explains_merge_overrides_youtube_metadata(self):
        job = WorkflowJob(
            name="Merge Job",
            source_mode="files",
            files=[
                FileEntry(source_path="/tmp/a.mp4", merge_group_id="graph-merge", graph_source_id="source-files-1"),
            ],
            upload_youtube=True,
            graph_nodes=[
                {"id": "source-files-1", "type": "source_files"},
                {"id": "merge-1", "type": "merge"},
                {"id": "upload-1", "type": "youtube_upload"},
            ],
            graph_edges=[
                {"source": "source-files-1", "target": "merge-1"},
                {"source": "merge-1", "target": "upload-1"},
            ],
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._sync_editor_state(sync_graph=False)

        assert "finalen YouTube-Metadaten" in dlg._editor_hint.text()

    def test_preview_mode_has_no_editor_controls(self):
        job = _make_job()
        dlg = JobWorkflowDialog(None, job, allow_edit=False, settings=_settings())

        assert not hasattr(dlg, "_name_edit")

    def test_preview_mode_builds_graph_from_job_state(self):
        job = _make_job(convert_enabled=True, upload_youtube=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=False, settings=_settings())

        node_types = {node.node_type for node in dlg._graph_view.node_items()}

        assert {"source_files", "convert", "youtube_upload"}.issubset(node_types)

    def test_runtime_sync_updates_node_progress_and_current_step(self):
        job = _make_job(convert_enabled=True, upload_youtube=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        job.resume_status = "Konvertiere …"
        job.step_statuses = {"transfer": "done", "convert": "running"}
        job.current_step_key = "convert"
        job.progress_pct = 37
        job.overall_progress_pct = 42

        dlg._sync_runtime_state_from_job()

        convert_node = _node_by_type(dlg._graph_view, "convert")
        assert "37%" in convert_node._state.toPlainText()
        assert dlg._current_label.text() == "Aktiver Step: Konvertierung"
        assert dlg._overall_label.text() == "Gesamtfortschritt: 42%"

    def test_runtime_sync_updates_source_node_transfer_progress(self):
        job = _make_job(convert_enabled=True, upload_youtube=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        job.step_statuses = {"transfer": "done", "yt_version": "running"}
        job.current_step_key = "yt_version"
        job.progress_pct = 6
        job.transfer_progress_pct = 100

        dlg._sync_runtime_state_from_job()

        source_node = _node_by_type(dlg._graph_view, "source_files")
        assert "100%" in source_node._state.toPlainText()

    def test_progress_fill_is_visible_in_drag_preview(self):
        job = _make_job(convert_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        pixmap = dlg._node_palette._create_drag_pixmap("convert")
        image = pixmap.toImage()
        sample = QColor(image.pixelColor(30, 30))

        assert sample != QColor("#FFFFFF")

    def test_header_shows_merge_warning_triangle_with_tooltip(self):
        job = WorkflowJob(
            name="Warnung",
            source_mode="files",
            files=[FileEntry(source_path="/tmp/a.mp4", merge_group_id="graph-merge")],
        )
        with patch("src.job_workflow.dialog.job_merge_warning", return_value="Nicht mergebar"):
            dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        assert dlg._merge_warning_label.isHidden() is False
        assert dlg._merge_warning_label.toolTip() == "Nicht mergebar"

    def test_merge_node_exposes_shared_output_configuration(self):
        job = WorkflowJob(
            name="Merge Konfig",
            source_mode="files",
            files=[
                FileEntry(source_path="/tmp/a.mp4", merge_group_id="graph-merge", graph_source_id="source-files-1"),
            ],
            graph_nodes=[
                {"id": "source-files-1", "type": "source_files"},
                {"id": "merge-1", "type": "merge"},
                {"id": "upload-1", "type": "youtube_upload"},
            ],
            graph_edges=[
                {"source": "source-files-1", "target": "merge-1"},
                {"source": "merge-1", "target": "upload-1"},
            ],
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._on_graph_selection_changed({"kind": "node", "id": "merge-1", "type": "merge"})

        assert dlg._property_stack.currentIndex() == dlg._property_pages["merge"]

        dlg._merge_panel.set_kaderblick_options(
            [{"id": 7, "name": "1. Halbzeit"}],
            [{"id": 9, "name": "Kamera 1"}],
        )
        dlg._merge_panel._date_edit.setDate(QDate(2026, 3, 22))
        dlg._merge_panel._competition_combo.setCurrentText("Liga")
        dlg._merge_panel._home_combo.setCurrentText("Heim")
        dlg._merge_panel._away_combo.setCurrentText("Gast")
        dlg._merge_panel._camera_combo.setCurrentIndex(dlg._merge_panel._camera_combo.findData(9))
        for button in dlg._merge_panel._side_group.buttons():
            if button.property("side_value") == "Links":
                button.click()
                break
        dlg._merge_panel._video_type_combo.setCurrentIndex(dlg._merge_panel._video_type_combo.findData(7))
        dlg._apply_and_accept()

        assert job.merge_output_title == "2026-03-22 | Heim vs Gast | Kamera 1 | Links 1. Halbzeit"
        assert job.merge_output_playlist == "22.03.2026 | Liga | Heim vs Gast"
        assert "22.03.2026" in job.merge_output_description
        assert "Heim vs Gast" in job.merge_output_description
        assert "Liga" in job.merge_output_description
        assert "Kamera 1" in job.merge_output_description
        assert job.merge_match_data["competition"] == "Liga"
        assert job.merge_segment_data["camera"] == "Kamera 1"
        assert job.merge_output_kaderblick_video_type_id == 7
        assert job.merge_output_kaderblick_camera_id == 9

    def test_merge_node_persists_freeform_camera_name_without_api_options(self):
        job = WorkflowJob(
            name="Merge Konfig",
            source_mode="files",
            files=[
                FileEntry(source_path="/tmp/a.mp4", merge_group_id="graph-merge", graph_source_id="source-files-1"),
            ],
            graph_nodes=[
                {"id": "source-files-1", "type": "source_files"},
                {"id": "merge-1", "type": "merge"},
                {"id": "upload-1", "type": "youtube_upload"},
            ],
            graph_edges=[
                {"source": "source-files-1", "target": "merge-1"},
                {"source": "merge-1", "target": "upload-1"},
            ],
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._on_graph_selection_changed({"kind": "node", "id": "merge-1", "type": "merge"})

        dlg._merge_panel._date_edit.setDate(QDate(2026, 3, 22))
        dlg._merge_panel._competition_combo.setCurrentText("Liga")
        dlg._merge_panel._home_combo.setCurrentText("Heim")
        dlg._merge_panel._away_combo.setCurrentText("Gast")
        dlg._merge_panel._camera_combo.setEditText("DJI Osmo Action 5 Pro")
        dlg._merge_panel._video_type_combo.setCurrentIndex(0)
        dlg._apply_and_accept()

        assert job.merge_segment_data["camera"] == "DJI Osmo Action 5 Pro"

        reopened = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())
        reopened._on_graph_selection_changed({"kind": "node", "id": "merge-1", "type": "merge"})

        assert reopened._merge_panel._camera_combo.currentText() == "DJI Osmo Action 5 Pro"

    def test_node_visual_state_uses_distinct_idle_and_progress_fills(self):
        pending_job = _make_job(convert_enabled=True)
        pending_visual = _node_visual_state("convert", pending_job)

        running_job = _make_job(convert_enabled=True)
        running_job.step_statuses = {"convert": "running"}
        running_job.current_step_key = "convert"
        running_job.progress_pct = 45
        running_visual = _node_visual_state("convert", running_job)

        done_job = _make_job()
        done_job.step_statuses = {"transfer": "done"}
        done_job.transfer_progress_pct = 100
        done_visual = _node_visual_state("source_files", done_job)

        assert pending_visual["fill_color"] == QColor("#FFFFFF")
        assert running_visual["fill_color"] == QColor("#FFFFFF")
        assert running_visual["progress_fill_color"] != running_visual["fill_color"]
        assert float(running_visual["progress_fraction"]) == 0.45
        assert done_visual["fill_color"] != QColor("#FFFFFF")
        assert float(done_visual["progress_fraction"]) == 1.0

    def test_editor_applies_step_options_and_files(self):
        job = _make_job(title_card_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._kb_video_type_options = [{"id": 3, "name": "Halbzeit"}]
        dlg._kb_camera_options = [{"id": 4, "name": "Hauptkamera"}]
        dlg._sync_kaderblick_selectors()

        dlg._yt_title_edit.setText("Liga Spiel")
        dlg._yt_playlist_edit.setText("Playlist 1")
        dlg._yt_competition_edit.setText("Pokal")
        dlg._kb_game_id_edit.setText("77")
        dlg._kb_type_combo.setCurrentIndex(dlg._kb_type_combo.findData(3))
        dlg._kb_camera_combo.setCurrentIndex(dlg._kb_camera_combo.findData(4))
        dlg._tc_home_edit.setText("Heim")
        dlg._tc_away_edit.setText("Gast")
        dlg._tc_date_edit.setText("2026-03-22")
        dlg._tc_logo_edit.setText("/tmp/logo.png")
        dlg._tc_bg_edit.setText("#112233")
        dlg._tc_fg_edit.setText("#FFFFFF")
        dlg._tc_duration_spin.setValue(4.5)
        dlg._encoder_combo.setCurrentIndex(max(dlg._encoder_combo.findData("libx264"), 0))
        dlg._preset_combo.setCurrentText("slow")
        dlg._crf_spin.setValue(21)
        dlg._fps_spin.setValue(30)
        dlg._format_combo.setCurrentText("avi")
        dlg._overwrite_cb.setChecked(True)
        dlg._merge_audio_cb.setChecked(True)
        dlg._amplify_audio_cb.setChecked(True)
        dlg._amplify_db_spin.setValue(8.0)
        dlg._audio_sync_cb.setChecked(True)
        with patch("src.ui.file_list_widget.QMessageBox.question", return_value=0x00000400), patch("src.ui.file_list_widget.QMessageBox.information"):
            dlg._file_list_widget._table.selectRow(0)
            dlg._file_list_widget._open_add_files_dialog = lambda: None

        dlg._apply_and_accept()

        assert job.default_youtube_title == "Liga Spiel"
        assert job.default_youtube_playlist == "Playlist 1"
        assert job.default_youtube_competition == "Pokal"
        assert job.default_kaderblick_game_id == "77"
        assert job.default_kaderblick_video_type_id == 3
        assert job.default_kaderblick_camera_id == 4
        assert job.title_card_home_team == "Heim"
        assert job.title_card_away_team == "Gast"
        assert job.title_card_date
        assert job.title_card_date.count("-") == 2
        assert job.title_card_logo_path == "/tmp/logo.png"
        assert job.title_card_bg_color == "#112233"
        assert job.title_card_fg_color == "#FFFFFF"
        assert job.title_card_duration == 4.5
        assert job.encoder == "libx264"
        assert job.preset == "slow"
        assert job.crf == 21
        assert job.fps == 30
        assert job.output_format == "avi"
        assert job.overwrite is True
        assert job.merge_audio is True
        assert job.amplify_audio is True
        assert job.amplify_db == 8.0
        assert job.audio_sync is True

    def test_folder_source_editor_provides_directory_picker_for_source_and_target(self):
        job = WorkflowJob(name="Ordner", source_mode="folder_scan")
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        with patch("src.job_workflow.dialog.QFileDialog.getExistingDirectory", side_effect=["/quelle", "/ziel"]):
            dlg._browse_dir(dlg._folder_src_edit, "Quellordner wählen")
            dlg._browse_dir(dlg._folder_dst_edit, "Zielordner wählen")

        assert dlg._folder_src_edit.text() == "/quelle"
        assert dlg._folder_dst_edit.text() == "/ziel"
        assert dlg._draft.source_folder == "/quelle"
        assert dlg._draft.copy_destination == "/ziel"

    def test_folder_source_editor_uses_last_directory_as_browse_default_and_persists_selection(self):
        settings = _settings()
        settings.last_directory = "/media/video/spieltag-23"
        dlg = JobWorkflowDialog(None, WorkflowJob(name="Ordner", source_mode="folder_scan"), allow_edit=True, settings=settings)

        with patch("src.job_workflow.dialog.QFileDialog.getExistingDirectory", return_value="/media/video/spieltag-24") as picker:
            dlg._browse_dir(dlg._folder_src_edit, "Quellordner wählen")

        picker.assert_called_once()
        assert picker.call_args.args[2] == "/media/video/spieltag-23"
        assert settings.last_directory == "/media/video/spieltag-24"
        assert dlg._folder_src_edit.text() == "/media/video/spieltag-24"

    def test_files_source_editor_provides_directory_picker_for_target(self):
        job = WorkflowJob(name="Dateien", source_mode="files", files=[FileEntry(source_path="/tmp/a.mp4")])
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        with patch("src.job_workflow.dialog.QFileDialog.getExistingDirectory", return_value="/ziel"):
            dlg._browse_dir(dlg._files_dst_edit, "Zielordner wählen")

        assert dlg._files_dst_edit.text() == "/ziel"
        assert dlg._draft.copy_destination == "/ziel"

    def test_files_source_editor_prefers_current_field_then_last_directory_for_target_browse(self):
        settings = _settings()
        settings.last_directory = "/media/video/spieltag-23"
        job = WorkflowJob(name="Dateien", source_mode="files", files=[FileEntry(source_path="/tmp/a.mp4")])
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        with patch("src.job_workflow.dialog.QFileDialog.getExistingDirectory", return_value="/media/video/export") as picker:
            dlg._browse_dir(dlg._files_dst_edit, "Zielordner wählen")

        picker.assert_called_once()
        assert picker.call_args.args[2] == "/media/video/spieltag-23"
        assert settings.last_directory == "/media/video/export"
        assert dlg._files_dst_edit.text() == "/media/video/export"

    def test_editor_tracks_merge_changes_from_graph(self):
        job = WorkflowJob(
            name="Merge Job",
            source_mode="files",
            files=[
                FileEntry(source_path="/tmp/a.mp4", graph_source_id="source-a"),
                FileEntry(source_path="/tmp/b.mp4", graph_source_id="source-b"),
            ],
            convert_enabled=True,
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        source_a = dlg._graph_view.add_node("source_files", node_id="source-a")
        source_b = dlg._graph_view.add_node("source_files", node_id="source-b")
        merge_id = dlg._graph_view.add_node("merge")
        dlg._graph_view.connect_nodes(source_a, merge_id)
        dlg._graph_view.connect_nodes(source_b, merge_id)
        dlg._sync_draft_from_graph(refresh_graph=False)

        assert "merge" in _planned_job_steps(dlg._draft)
        assert "Merge ist aktiv" in dlg._merge_label.text()

    def test_adding_files_does_not_rebuild_existing_graph(self):
        job = WorkflowJob(
            name="Merge Job",
            source_mode="files",
            files=[FileEntry(source_path="/tmp/a.mp4")],
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files", node_id="source-files")
        merge_id = dlg._graph_view.add_node("merge", node_id="merge-node")
        upload_id = dlg._graph_view.add_node("youtube_upload", node_id="upload-node")
        dlg._graph_view.connect_nodes(source_id, merge_id)
        dlg._graph_view.connect_nodes(merge_id, upload_id)
        node_ids_before = {node_id for node_id, _node in dlg._graph_view.node_entries()}
        edges_before = set(dlg._graph_view.edge_pairs())

        assert dlg._file_list_widget is not None
        dlg._file_list_widget.load([
            FileEntry(source_path="/tmp/a.mp4"),
            FileEntry(source_path="/tmp/b.mp4"),
        ])

        assert {node_id for node_id, _node in dlg._graph_view.node_entries()} == node_ids_before
        assert set(dlg._graph_view.edge_pairs()) == edges_before
        assert dlg._graph_view.node_item("merge-node") is not None
        assert dlg._draft.upload_youtube is True

    def test_editor_edits_folder_scan_source_fields(self):
        job = WorkflowJob(
            name="Ordner Job",
            source_mode="folder_scan",
            source_folder="/input",
            file_pattern="*.mov",
            copy_destination="/output",
            move_files=False,
            output_prefix="A_",
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        assert dlg._source_mode_widgets["folder_scan"].isHidden() is False
        dlg._folder_src_edit.setText("/spiele")
        dlg._file_pattern_edit.setText("*.mp4")
        dlg._folder_dst_edit.setText("/fertig")
        dlg._move_files_cb.setChecked(True)
        dlg._folder_prefix_edit.setText("B_")
        dlg._apply_and_accept()

        assert job.source_folder == "/spiele"
        assert job.file_pattern == "*.mp4"
        assert job.copy_destination == "/fertig"
        assert job.move_files is True
        assert job.output_prefix == "B_"

    def test_editor_edits_pi_source_fields(self):
        settings = _settings()
        if settings.cameras.devices:
            device_name = settings.cameras.devices[0].name
        else:
            device_name = ""
        job = WorkflowJob(
            name="Pi Job",
            source_mode="pi_download",
            device_name=device_name,
            download_destination="/downloads",
            delete_after_download=False,
            output_prefix="cam_",
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        assert dlg._source_mode_widgets["pi_download"].isHidden() is False
        if dlg._device_combo.count() > 1:
            dlg._device_combo.setCurrentIndex(1)
            expected_device = dlg._device_combo.currentData()
        else:
            expected_device = ""
        dlg._pi_dest_edit.setText("/neu")
        dlg._delete_after_dl_cb.setChecked(True)
        dlg._pi_prefix_edit.setText("kb_")
        dlg._apply_and_accept()

        assert job.device_name == expected_device
        assert job.download_destination == "/neu"
        assert job.delete_after_download is True
        assert job.output_prefix == "kb_"

    def test_editor_disables_upload_detail_fields_when_upload_off(self):
        job = _make_job(upload_youtube=True, upload_kaderblick=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        dlg._graph_view.add_node("source_files")

        assert dlg._yt_title_edit.isEnabled() is False
        assert dlg._yt_playlist_edit.isEnabled() is False
        assert dlg._yt_competition_edit.isEnabled() is False
        assert dlg._kb_game_id_edit.isEnabled() is False
        assert dlg._kb_type_combo.isEnabled() is False
        assert dlg._kb_camera_combo.isEnabled() is False

    def test_editor_switches_to_node_specific_inspector_pages(self):
        job = _make_job(upload_youtube=True, upload_kaderblick=True, title_card_enabled=True, convert_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._on_graph_selection_changed({"kind": "node", "type": "titlecard"})
        assert dlg._property_stack.currentIndex() == dlg._property_pages["titlecard"]

        dlg._on_graph_selection_changed({"kind": "node", "type": "youtube_upload"})
        assert dlg._property_stack.currentIndex() == dlg._property_pages["youtube_upload"]

        dlg._on_graph_selection_changed({"kind": "node", "type": "kaderblick"})
        assert dlg._property_stack.currentIndex() == dlg._property_pages["kaderblick"]

        dlg._on_graph_selection_changed({"kind": "node", "type": "cleanup"})
        assert dlg._property_stack.currentIndex() == dlg._property_pages["cleanup"]

        dlg._on_graph_selection_changed({"kind": "node", "type": "stop"})
        assert dlg._property_stack.currentIndex() == dlg._property_pages["stop"]

    def test_kaderblick_inspector_uses_distinct_heading_and_form_label(self):
        dlg = JobWorkflowDialog(None, _make_job(upload_youtube=True, upload_kaderblick=True), allow_edit=True, settings=_settings())

        kaderblick_page = dlg._property_stack.widget(dlg._property_pages["kaderblick"])

        assert kaderblick_page.title() == "Kaderblick"
        label_texts = {label.text() for label in kaderblick_page.findChildren(QLabel)}
        assert "API-Daten:" in label_texts
        assert "Kaderblick:" not in label_texts
        assert "Spiel-ID:" in label_texts
        assert "Kaderblick-Video-Typ:" in label_texts
        assert "Kaderblick-Kamera:" in label_texts

    def test_editor_hint_warns_when_irreparable_branch_is_unhandled(self):
        job = _make_job(convert_enabled=False)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files", node_id="source-files-1")
        validate_id = dlg._graph_view.add_node("validate_surface", node_id="validate-1")
        yt_id = dlg._graph_view.add_node("yt_version", node_id="yt-1")
        dlg._graph_view.connect_nodes(source_id, validate_id)
        dlg._graph_view.connect_nodes(validate_id, yt_id, "ok")
        dlg._sync_draft_from_graph(refresh_graph=False)
        dlg._sync_editor_state(sync_graph=False)

        assert "irreparabel-Branch" in dlg._editor_hint.text()

    def test_merge_selection_loads_merge_kaderblick_api_data_and_restores_camera_dropdown(self):
        settings = _settings()
        settings.kaderblick.auth_mode = "bearer"
        settings.kaderblick.bearer_token = "token"
        job = WorkflowJob(
            name="Merge Restore",
            source_mode="files",
            files=[FileEntry(source_path="/tmp/a.mp4", merge_group_id="graph-merge", graph_source_id="source-files-1")],
            merge_match_data={
                "date_iso": "2026-03-21",
                "competition": "Liga",
                "home_team": "Heim",
                "away_team": "Gast",
            },
            merge_segment_data={
                "camera": "DJI Osmo Action 5 Pro",
                "side": "",
                "half": 1,
                "part": 0,
                "type_name": "1. Halbzeit",
            },
            merge_output_kaderblick_video_type_id=2,
            merge_output_kaderblick_camera_id=1,
            graph_nodes=[
                {"id": "source-files-1", "type": "source_files"},
                {"id": "merge-1", "type": "merge"},
                {"id": "upload-1", "type": "youtube_upload"},
            ],
            graph_edges=[
                {"source": "source-files-1", "target": "merge-1"},
                {"source": "merge-1", "target": "upload-1"},
            ],
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        with patch("src.job_workflow.dialog.fetch_video_types", return_value=[{"id": 2, "name": "1. Halbzeit"}]), patch(
            "src.job_workflow.dialog.fetch_cameras", return_value=[{"id": 1, "name": "DJI Osmo Action 5 Pro"}]
        ):
            dlg._on_graph_selection_changed({"kind": "node", "id": "merge-1", "type": "merge"})

        assert dlg._property_stack.currentIndex() == dlg._property_pages["merge"]
        assert dlg._merge_panel._camera_combo.currentData() == 1
        assert dlg._merge_panel._camera_combo.currentText() == "DJI Osmo Action 5 Pro"
        assert dlg._merge_panel._video_type_combo.currentData() == 2

    def test_youtube_upload_panel_hides_standard_fields_when_merge_output_metadata_is_relevant(self):
        job = WorkflowJob(
            name="Merge Upload",
            source_mode="files",
            files=[
                FileEntry(source_path="/tmp/a.mp4", merge_group_id="graph-merge", graph_source_id="source-files-1"),
            ],
            graph_nodes=[
                {"id": "source-files-1", "type": "source_files"},
                {"id": "merge-1", "type": "merge"},
                {"id": "upload-1", "type": "youtube_upload"},
            ],
            graph_edges=[
                {"source": "source-files-1", "target": "merge-1"},
                {"source": "merge-1", "target": "upload-1"},
            ],
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._sync_editor_state(sync_graph=False)

        assert dlg._youtube_panel.is_merge_output_mode() is True
        assert dlg._youtube_panel._standard_fields.isHidden() is True

    def test_merge_panel_stays_available_without_upload_for_local_output_metadata(self):
        job = WorkflowJob(
            name="Merge Lokal",
            source_mode="files",
            files=[
                FileEntry(source_path="/tmp/a.mp4", merge_group_id="graph-merge", graph_source_id="source-files-1"),
            ],
            graph_nodes=[
                {"id": "source-files-1", "type": "source_files"},
                {"id": "merge-1", "type": "merge"},
            ],
            graph_edges=[
                {"source": "source-files-1", "target": "merge-1"},
            ],
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._on_graph_selection_changed({"kind": "node", "id": "merge-1", "type": "merge"})

        assert dlg._property_stack.currentIndex() == dlg._property_pages["merge"]
        assert dlg._merge_panel.isHidden() is False

    def test_graph_change_enables_youtube_fields_when_upload_node_becomes_reachable(self):
        job = _make_job(upload_youtube=False, upload_kaderblick=False, convert_enabled=False)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        assert dlg._yt_title_edit.isEnabled() is False

        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files")
        upload_id = dlg._graph_view.add_node("youtube_upload")
        dlg._graph_view.connect_nodes(source_id, upload_id)

        assert dlg._draft.upload_youtube is True
        assert dlg._yt_title_edit.isEnabled() is True
        assert dlg._yt_playlist_edit.isEnabled() is True
        assert dlg._yt_competition_edit.isEnabled() is True

    def test_graph_change_enables_kaderblick_fields_when_post_node_becomes_reachable(self):
        job = _make_job(upload_youtube=False, upload_kaderblick=False, convert_enabled=False)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        assert dlg._kb_game_id_edit.isEnabled() is False

        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files")
        upload_id = dlg._graph_view.add_node("youtube_upload")
        kb_id = dlg._graph_view.add_node("kaderblick")
        dlg._graph_view.connect_nodes(source_id, upload_id)
        dlg._graph_view.connect_nodes(upload_id, kb_id)

        assert dlg._draft.upload_youtube is True
        assert dlg._draft.upload_kaderblick is True
        assert dlg._kb_game_id_edit.isEnabled() is True
        assert dlg._kb_type_combo.isEnabled() is True
        assert dlg._kb_camera_combo.isEnabled() is True

    def test_editor_disables_titlecard_detail_fields_when_titlecard_off(self):
        job = _make_job(title_card_enabled=True, convert_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files")
        dlg._graph_view.add_node("convert")
        convert_id = _node_id_by_type(dlg._graph_view, "convert")
        dlg._graph_view.connect_nodes(source_id, convert_id)

        assert dlg._tc_home_edit.isEnabled() is False
        assert dlg._tc_logo_edit.isEnabled() is False
        assert dlg._tc_bg_edit.isEnabled() is False
        assert dlg._tc_fg_edit.isEnabled() is False

    def test_playlist_helper_updates_playlist_and_match_fields(self):
        class _DummyMatchData:
            competition = "Kreispokal"
            home_team = "FC Heim"
            away_team = "FC Gast"
            date_iso = "2026-03-22"

        class _DummyPlaylistDialog:
            def __init__(self, *args, **kwargs):
                self.playlist_title = "Kreispokal | FC Heim - FC Gast"
                self.match_data = _DummyMatchData()

            def exec(self):
                return True

        job = _make_job(upload_youtube=True, title_card_enabled=True, convert_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        with patch("src.job_workflow.dialog.YouTubeTitleEditorDialog", _DummyPlaylistDialog), patch("src.job_workflow.dialog.MatchData", SimpleNamespace):
            dlg._open_match_editor_for_playlist()

        assert dlg._yt_playlist_edit.text() == "Kreispokal | FC Heim - FC Gast"
        assert dlg._yt_competition_edit.text() == "Kreispokal"
        assert dlg._tc_home_edit.text() == "FC Heim"
        assert dlg._tc_away_edit.text() == "FC Gast"
        assert dlg._tc_date_edit.text() == "2026-03-22"

    def test_kaderblick_loader_updates_status_and_file_widgets(self):
        settings = _settings()
        settings.kaderblick.jwt_token = "token"
        job = _make_job(upload_youtube=True, upload_kaderblick=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        with patch("src.job_workflow.dialog.fetch_video_types", return_value=[{"id": 1}]), patch("src.job_workflow.dialog.fetch_cameras", return_value=[{"id": 2}]):
            dlg._kb_load_api_data(force=True)

        assert "1 Typen, 1 Kameras geladen" in dlg._kb_status_label.text()

    def test_kaderblick_loader_retries_after_missing_token_was_fixed(self):
        settings = _settings()
        settings.kaderblick.auth_mode = "bearer"
        settings.kaderblick.bearer_token = ""
        job = _make_job(upload_youtube=True, upload_kaderblick=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        dlg._kb_load_api_data()
        assert "Kein Bearer-Token konfiguriert" in dlg._kb_status_label.text()

        settings.kaderblick.bearer_token = "token"
        with patch("src.job_workflow.dialog.fetch_video_types", return_value=[{"id": 1}]), patch("src.job_workflow.dialog.fetch_cameras", return_value=[{"id": 2}]):
            dlg._kb_load_api_data()

        assert "1 Typen, 1 Kameras geladen" in dlg._kb_status_label.text()

    def test_pi_loader_populates_selectable_file_list(self):
        settings = _settings()
        settings.cameras.devices = [SimpleNamespace(name="Pi 1", ip="10.0.0.5")]
        settings.cameras.destination = "/dest"
        job = WorkflowJob(source_mode="pi_download", device_name="Pi 1", download_destination="/dest")
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        dlg._device_combo.setCurrentIndex(1)
        dlg._on_camera_files_loaded([{"base": "halbzeit1"}, {"base": "halbzeit2"}])

        assert dlg._pi_file_list.isHidden() is False
        assert len(dlg._draft.files) == 2
        assert dlg._draft.files[0].source_path.endswith("/dest/Pi 1/halbzeit1.mjpg")