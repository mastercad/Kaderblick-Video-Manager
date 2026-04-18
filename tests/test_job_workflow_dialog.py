import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from PySide6.QtCore import QDate, QPoint, QPointF, Qt
from PySide6.QtGui import QColor, QWheelEvent
from PySide6.QtWidgets import QApplication, QLabel, QMessageBox, QVBoxLayout

from src.job_workflow.graph.edge_item import _GraphEdgeItem
from src.job_workflow.graph.builder import build_default_graph
from src.job_workflow.graph.geometry import auto_layout_graph, build_connection_path
from src.job_workflow.graph.view import _WorkflowGraphView
from src.settings import AppSettings
from src.workflow import FileEntry, WorkflowJob
from src.job_workflow.dialog import JobWorkflowDialog, _node_visual_state, _planned_job_steps
from src.job_workflow.panels.status import WorkflowNotesPanel


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


def _send_wheel(widget, *, delta_y: int = 120) -> None:
    local_pos = QPointF(widget.rect().center())
    global_pos = QPointF(widget.mapToGlobal(widget.rect().center()))
    event = QWheelEvent(
        local_pos,
        global_pos,
        QPoint(),
        QPoint(0, delta_y),
        Qt.MouseButton.NoButton,
        Qt.KeyboardModifier.NoModifier,
        Qt.ScrollPhase.ScrollUpdate,
        False,
    )
    QApplication.sendEvent(widget, event)
    QApplication.processEvents()


class TestJobWorkflowDialog:
    def test_workflow_notes_panel_replaces_dynamic_labels_without_leaving_old_widgets_visible(self):
        panel = WorkflowNotesPanel()
        job = _make_job(upload_youtube=True, upload_kaderblick=True)

        panel.refresh_from_job(job)
        panel.refresh_from_job(job)

        layout_texts = []
        for index in range(panel._layout.count()):
            item = panel._layout.itemAt(index)
            widget = item.widget() if item is not None else None
            if isinstance(widget, QLabel):
                layout_texts.append(widget.text())

        assert layout_texts.count("Hinweise zur Ausführung") == 1
        assert layout_texts.count("Step-Zusammenfassung") == 1

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

    def test_editor_sidebar_is_resizable_and_starts_usable(self):
        dlg = JobWorkflowDialog(None, _make_job(convert_enabled=True), allow_edit=True, settings=_settings())

        assert dlg.minimumWidth() >= 1220
        assert dlg.width() >= 1500
        assert dlg._palette_box.minimumWidth() >= 260
        assert dlg._inspector_box.minimumWidth() >= 480
        assert dlg._palette_box.maximumWidth() >= 16_000_000
        assert dlg._inspector_box.maximumWidth() >= 16_000_000

    def test_graph_view_zoom_stays_within_limits(self):
        dlg = JobWorkflowDialog(None, _make_job(convert_enabled=True), allow_edit=True, settings=_settings())
        graph_view = dlg._graph_view

        for _ in range(20):
            graph_view._apply_zoom_factor(graph_view.ZOOM_STEP)

        assert graph_view.transform().m11() <= graph_view.MAX_ZOOM + 1e-6

        for _ in range(40):
            graph_view._apply_zoom_factor(1 / graph_view.ZOOM_STEP)

        assert graph_view.transform().m11() >= graph_view.MIN_ZOOM - 1e-6

    def test_node_settings_only_change_on_wheel_when_field_has_focus(self):
        dlg = JobWorkflowDialog(None, _make_job(convert_enabled=True), allow_edit=True, settings=_settings())
        dlg.show()
        dlg._on_graph_selection_changed({"kind": "node", "type": "convert"})
        QApplication.processEvents()

        dlg._name_edit.setFocus()
        QApplication.processEvents()

        dlg._crf_spin.setValue(18)
        crf_before = dlg._crf_spin.value()
        _send_wheel(dlg._crf_spin)
        assert dlg._crf_spin.value() == crf_before

        assert dlg._preset_combo.count() >= 3
        dlg._preset_combo.setCurrentIndex(1)
        preset_before = dlg._preset_combo.currentIndex()
        _send_wheel(dlg._preset_combo)
        assert dlg._preset_combo.currentIndex() == preset_before

        dlg._crf_spin.setFocus()
        QApplication.processEvents()
        _send_wheel(dlg._crf_spin)
        assert dlg._crf_spin.value() != crf_before

        dlg._preset_combo.setFocus()
        QApplication.processEvents()
        _send_wheel(dlg._preset_combo)
        assert dlg._preset_combo.currentIndex() != preset_before

    def test_graph_view_middle_mouse_pan_updates_scrollbars(self):
        graph_view = _WorkflowGraphView()
        graph_view.resize(320, 240)
        graph_view.setSceneRect(0, 0, 2400, 1800)
        graph_view.show()
        QApplication.processEvents()
        graph_view.centerOn(1200, 900)
        QApplication.processEvents()

        start_h = graph_view.horizontalScrollBar().value()
        start_v = graph_view.verticalScrollBar().value()

        graph_view._start_panning(QPoint(160, 120))
        assert graph_view._is_panning is True
        assert graph_view.cursor().shape() == Qt.CursorShape.ClosedHandCursor

        graph_view._pan_to(QPoint(110, 80))

        assert graph_view.horizontalScrollBar().value() != start_h
        assert graph_view.verticalScrollBar().value() != start_v

        graph_view._stop_panning()

        assert graph_view._is_panning is False
        assert graph_view._last_pan_pos is None
        assert graph_view.cursor().shape() != Qt.CursorShape.ClosedHandCursor

    def test_graph_rebuild_fits_rightmost_delivery_nodes_into_view(self):
        job = _make_job(
            convert_enabled=False,
            upload_youtube=True,
            upload_kaderblick=True,
        )
        job.graph_nodes = [
            {"id": "source-1", "type": "source_files", "x": 80.0, "y": 100.0},
            {"id": "merge-1", "type": "merge", "x": 360.0, "y": 100.0},
            {"id": "upload-1", "type": "youtube_upload", "x": 680.0, "y": 100.0},
            {"id": "kb-1", "type": "kaderblick", "x": 680.0, "y": 232.0},
        ]
        job.graph_edges = [
            {"source": "source-1", "target": "merge-1"},
            {"source": "merge-1", "target": "upload-1"},
            {"source": "upload-1", "target": "kb-1"},
        ]

        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())
        dlg.show()
        QApplication.processEvents()

        kb_node = dlg._graph_view.node_item("kb-1")
        assert kb_node is not None

        top_left = dlg._graph_view.mapFromScene(kb_node.scenePos())
        right_edge = top_left.x() + int(kb_node.rect().width())

        assert top_left.x() >= 0
        assert right_edge <= dlg._graph_view.viewport().width()

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

    def test_reset_action_clears_selected_node_and_downstream_runtime_state(self, monkeypatch):
        source_path = Path("/tmp/reset-dialog.mp4")
        job = WorkflowJob(
            name="Workflow Job",
            source_mode="files",
            files=[FileEntry(source_path=str(source_path))],
            upload_youtube=True,
            upload_kaderblick=True,
            step_statuses={
                "transfer": "done",
                "convert": "done",
                "youtube_upload": "done",
                "kaderblick": "done",
            },
            resume_status="Kaderblick senden …",
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        monkeypatch.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.StandardButton.Yes)
        monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: QMessageBox.StandardButton.Ok)

        dlg._run_reset_action("youtube_upload")

        assert dlg.changed is True
        assert dlg._job.step_statuses["transfer"] == "done"
        assert dlg._job.step_statuses["convert"] == "done"
        assert "youtube_upload" not in dlg._job.step_statuses
        assert "kaderblick" not in dlg._job.step_statuses
        assert "youtube_upload" not in dlg._draft.step_statuses
        assert "kaderblick" not in dlg._draft.step_statuses

    def test_full_reset_confirmation_omits_warning_for_moved_sources(self, monkeypatch, tmp_path):
        source_path = tmp_path / "imports" / "clip.mp4"
        target_dir = tmp_path / "raw"
        target_path = target_dir / source_path.name
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text("x", encoding="utf-8")

        job = WorkflowJob(
            name="Workflow Job",
            source_mode="files",
            files=[FileEntry(source_path=str(source_path))],
            move_files=True,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "done"},
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())
        captured: dict[str, str] = {}

        def fake_question(_parent, _title, prompt, *_args, **_kwargs):
            captured["prompt"] = prompt
            return QMessageBox.StandardButton.No

        monkeypatch.setattr(QMessageBox, "question", fake_question)

        dlg._run_reset_action(None)

        assert "ACHTUNG" not in captured["prompt"]

    def test_partial_reset_confirmation_omits_moved_source_warning(self, monkeypatch, tmp_path):
        source_path = tmp_path / "imports" / "clip.mp4"
        target_dir = tmp_path / "raw"
        target_path = target_dir / source_path.name
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text("x", encoding="utf-8")

        job = WorkflowJob(
            name="Workflow Job",
            source_mode="files",
            files=[FileEntry(source_path=str(source_path))],
            move_files=True,
            copy_destination=str(target_dir),
            step_statuses={"transfer": "done", "convert": "done"},
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())
        captured: dict[str, str] = {}

        def fake_question(_parent, _title, prompt, *_args, **_kwargs):
            captured["prompt"] = prompt
            return QMessageBox.StandardButton.No

        monkeypatch.setattr(QMessageBox, "question", fake_question)

        dlg._run_reset_action("convert")

        assert "ACHTUNG" not in captured["prompt"]

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
        assert any(entry.merge_group_id for entry in dlg._draft.files)
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
        dlg._merge_panel._date_edit.setText("22.03.2026")
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

        dlg._merge_panel._date_edit.setText("22.03.2026")
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

    def test_source_node_visual_state_restores_100_percent_from_done_transfer_status(self):
        reopened_job = _make_job()
        reopened_job.step_statuses = {"transfer": "done"}
        reopened_job.transfer_progress_pct = 0

        visual = _node_visual_state("source_files", reopened_job)

        assert visual["state_text"].endswith("100%")
        assert float(visual["progress_fraction"]) == 1.0

    def test_editor_applies_step_options_and_files(self):
        job = _make_job(title_card_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._kb_video_type_options = [{"id": 3, "name": "Halbzeit"}]
        dlg._kb_camera_options = [{"id": 4, "name": "Hauptkamera"}]
        dlg._sync_kaderblick_selectors()

        dlg._youtube_metadata_panel._date_edit.setText("22.03.2026")
        dlg._youtube_metadata_panel._competition_combo.setCurrentText("Pokal")
        dlg._youtube_metadata_panel._home_combo.setCurrentText("Heim")
        dlg._youtube_metadata_panel._away_combo.setCurrentText("Gast")
        dlg._youtube_metadata_panel._camera_combo.setCurrentIndex(max(dlg._youtube_metadata_panel._camera_combo.findData(4), 0))
        dlg._youtube_metadata_panel._video_type_combo.setCurrentIndex(max(dlg._youtube_metadata_panel._video_type_combo.findData(3), 0))
        dlg._kb_game_id_edit.setText("77")
        dlg._tc_home_edit.setText("Heim")
        dlg._tc_away_edit.setText("Gast")
        dlg._tc_date_edit.setText("2026-03-22")
        dlg._tc_logo_edit.setText("/tmp/logo.png")
        dlg._tc_bg_edit.setText("#112233")
        dlg._tc_fg_edit.setText("#FFFFFF")
        dlg._tc_duration_spin.setValue(4.5)
        dlg._encoder_combo.setCurrentIndex(max(dlg._encoder_combo.findData("libx264"), 0))
        dlg._preset_combo.setCurrentText("slow")
        dlg._no_bframes_cb.setChecked(False)
        dlg._crf_spin.setValue(21)
        dlg._fps_spin.setValue(30)
        dlg._resolution_combo.setCurrentIndex(max(dlg._resolution_combo.findData("1080p"), 0))
        dlg._format_combo.setCurrentIndex(max(dlg._format_combo.findData("avi"), 0))
        dlg._merge_encoding_panel._encoder_combo.setCurrentIndex(max(dlg._merge_encoding_panel._encoder_combo.findData("libx264"), 0))
        dlg._merge_encoding_panel._crf_spin.setValue(19)
        dlg._merge_encoding_panel._preset_combo.setCurrentText("slower")
        dlg._merge_encoding_panel._no_bframes_cb.setChecked(False)
        dlg._merge_encoding_panel._fps_spin.setValue(50)
        dlg._merge_encoding_panel._resolution_combo.setCurrentIndex(max(dlg._merge_encoding_panel._resolution_combo.findData("720p"), 0))
        dlg._merge_encoding_panel._format_combo.setCurrentIndex(max(dlg._merge_encoding_panel._format_combo.findData("avi"), 0))
        dlg._yt_version_panel._encoding_panel._encoder_combo.setCurrentIndex(max(dlg._yt_version_panel._encoding_panel._encoder_combo.findData("libx264"), 0))
        dlg._yt_version_panel._encoding_panel._crf_spin.setValue(17)
        dlg._yt_version_panel._encoding_panel._preset_combo.setCurrentText("veryslow")
        dlg._yt_version_panel._encoding_panel._no_bframes_cb.setChecked(True)
        dlg._yt_version_panel._encoding_panel._fps_spin.setValue(60)
        dlg._yt_version_panel._encoding_panel._resolution_combo.setCurrentIndex(max(dlg._yt_version_panel._encoding_panel._resolution_combo.findData("2160p"), 0))
        dlg._yt_version_panel._encoding_panel._format_combo.setCurrentIndex(max(dlg._yt_version_panel._encoding_panel._format_combo.findData("mp4"), 0))
        dlg._overwrite_cb.setChecked(True)
        dlg._merge_audio_cb.setChecked(True)
        dlg._amplify_audio_cb.setChecked(True)
        dlg._amplify_db_spin.setValue(8.0)
        dlg._audio_sync_cb.setChecked(True)
        with patch("src.ui.file_list_widget.QMessageBox.question", return_value=0x00000400), patch("src.ui.file_list_widget.QMessageBox.information"):
            dlg._file_list_widget._table.selectRow(0)
            dlg._file_list_widget._open_add_files_dialog = lambda: None

        dlg._apply_and_accept()

        assert job.default_youtube_title == "2026-03-22 | Heim vs Gast | Hauptkamera | Halbzeit"
        assert job.default_youtube_playlist == "22.03.2026 | Pokal | Heim vs Gast"
        assert "Pokal" in job.default_youtube_description
        assert job.default_youtube_competition == "Pokal"
        assert job.youtube_match_data["competition"] == "Pokal"
        assert job.youtube_segment_data["camera"] == "Hauptkamera"
        assert job.default_kaderblick_game_id == "77"
        assert job.default_kaderblick_video_type_id == 3
        assert job.default_kaderblick_camera_id == 4
        assert dlg._kb_type_combo.currentData() == 3
        assert dlg._kb_camera_combo.currentData() == 4
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
        assert job.no_bframes is False
        assert job.crf == 21
        assert job.fps == 30
        assert job.output_resolution == "1080p"
        assert job.output_format == "avi"
        assert job.merge_encoder == "libx264"
        assert job.merge_crf == 19
        assert job.merge_preset == "slower"
        assert job.merge_no_bframes is False
        assert job.merge_fps == 50
        assert job.merge_output_resolution == "720p"
        assert job.merge_output_format == "avi"
        assert job.yt_version_encoder == "libx264"
        assert job.yt_version_crf == 17
        assert job.yt_version_preset == "veryslow"
        assert job.yt_version_no_bframes is True
        assert job.yt_version_fps == 60
        assert job.yt_version_output_resolution == "2160p"
        assert job.yt_version_output_format == "mp4"
        assert job.overwrite is True
        assert job.merge_audio is True
        assert job.amplify_audio is True
        assert job.amplify_db == 8.0
        assert job.audio_sync is True

    def test_editor_does_not_persist_inherited_defaults_as_node_overrides(self):
        settings = _settings()
        settings.default_match_date = "2026-03-22"
        settings.default_match_competition = "Pokal"
        settings.default_match_home_team = "FC Heim"
        settings.default_match_away_team = "FC Gast"
        settings.default_match_location = "Sportplatz Mitte"
        settings.default_kaderblick_game_id = "77"

        job = _make_job(upload_youtube=True, upload_kaderblick=True, title_card_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        assert dlg._kb_game_id_edit.text() == ""
        assert dlg._kb_game_id_edit.placeholderText() == "77"
        assert dlg._tc_home_edit.text() == ""
        assert dlg._tc_home_edit.placeholderText() == "FC Heim"
        assert dlg._youtube_metadata_panel._competition_combo.currentText() == ""
        assert dlg._youtube_metadata_panel._competition_combo.lineEdit().placeholderText() == "Pokal"
        assert dlg._youtube_metadata_panel._competition_combo.lineEdit().styleSheet() == ""

        dlg._youtube_metadata_panel._camera_combo.setEditText("Hauptkamera")
        dlg._apply_and_accept()

        assert job.default_kaderblick_game_id == ""
        assert job.title_card_home_team == ""
        assert job.title_card_away_team == ""
        assert job.title_card_date == ""
        assert job.youtube_match_data == {}
        assert job.default_youtube_title == "2026-03-22 | FC Heim vs FC Gast | Hauptkamera | 1. Halbzeit"

    def test_step_encoding_panels_show_source_material_summary(self, tmp_path):
        source = tmp_path / "source.mp4"
        source.write_text("video", encoding="utf-8")
        job = WorkflowJob(name="Quelle", source_mode="files", files=[FileEntry(source_path=str(source))])

        with patch("src.job_workflow.panels.inspector.get_video_stream_info", return_value={"codec_name": "h264", "fps": 50.0, "bit_rate": 1000}), \
             patch("src.job_workflow.panels.inspector.get_resolution", return_value=(1920, 1080)), \
             patch("src.job_workflow.panels.inspector.has_audio_stream", return_value=True):
            dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        assert "1920x1080" in dlg._merge_encoding_panel._source_info_label.text()
        assert "50.000 fps" in dlg._merge_encoding_panel._source_info_label.text()
        assert "50.000 fps" in dlg._yt_version_panel._encoding_panel._effective_info_label.text()

    def test_merge_panel_uses_convert_output_as_input_summary(self, tmp_path):
        source = tmp_path / "source.mjpg"
        source.write_text("raw", encoding="utf-8")
        job = WorkflowJob(
            name="Kaderblick",
            source_mode="pi_download",
            convert_enabled=True,
            output_format="mp4",
            output_resolution="1080p",
            fps=25,
            files=[FileEntry(source_path=str(source))],
            graph_nodes=[
                {"id": "source-1", "type": "source_pi_download"},
                {"id": "convert-1", "type": "convert"},
                {"id": "merge-1", "type": "merge"},
            ],
            graph_edges=[
                {"source": "source-1", "target": "convert-1"},
                {"source": "convert-1", "target": "merge-1"},
            ],
        )

        with patch("src.job_workflow.panels.inspector.get_video_stream_info") as stream_info, \
             patch("src.job_workflow.panels.inspector.get_resolution") as resolution, \
             patch("src.job_workflow.panels.inspector.has_audio_stream") as audio:
            dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        assert "Input aus Konvertieren" in dlg._merge_encoding_panel._source_info_label.text()
        assert "MP4" in dlg._merge_encoding_panel._source_info_label.text()
        assert "Full HD" in dlg._merge_encoding_panel._source_info_label.text()
        assert "25 fps" in dlg._merge_encoding_panel._source_info_label.text()
        assert not dlg._merge_encoding_panel._refresh_source_btn.isVisible()
        assert dlg._merge_encoding_panel._encoder_combo.itemText(0) == "Von Konvertieren übernehmen"
        assert dlg._merge_encoding_panel._crf_spin.specialValueText() == "Von Konvertieren übernehmen"
        assert dlg._merge_encoding_panel._fps_spin.specialValueText() == "Von Konvertieren übernehmen"
        assert dlg._merge_encoding_panel._format_combo.itemText(0) == "Von Konvertieren übernehmen"
        assert dlg._merge_encoding_panel._resolution_combo.itemText(0) == "Von Konvertieren übernehmen"

    def test_yt_version_panel_uses_merge_output_as_input_summary(self, tmp_path):
        source = tmp_path / "source.mjpg"
        source.write_text("raw", encoding="utf-8")
        job = WorkflowJob(
            name="YT nach Merge",
            source_mode="pi_download",
            convert_enabled=True,
            output_format="mp4",
            output_resolution="1080p",
            fps=25,
            merge_output_format="avi",
            merge_output_resolution="720p",
            merge_fps=50,
            files=[FileEntry(source_path=str(source))],
            graph_nodes=[
                {"id": "source-1", "type": "source_pi_download"},
                {"id": "convert-1", "type": "convert"},
                {"id": "merge-1", "type": "merge"},
                {"id": "yt-1", "type": "yt_version"},
            ],
            graph_edges=[
                {"source": "source-1", "target": "convert-1"},
                {"source": "convert-1", "target": "merge-1"},
                {"source": "merge-1", "target": "yt-1"},
            ],
        )

        with patch("src.job_workflow.panels.inspector.get_video_stream_info") as stream_info, \
             patch("src.job_workflow.panels.inspector.get_resolution") as resolution, \
             patch("src.job_workflow.panels.inspector.has_audio_stream") as audio:
            dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        assert "Input aus Merge" in dlg._yt_version_panel._encoding_panel._source_info_label.text()
        assert "AVI" in dlg._yt_version_panel._encoding_panel._source_info_label.text()
        assert "HD" in dlg._yt_version_panel._encoding_panel._source_info_label.text()
        assert "50 fps" in dlg._yt_version_panel._encoding_panel._source_info_label.text()
        stream_info.assert_not_called()
        resolution.assert_not_called()
        audio.assert_not_called()
        assert dlg._yt_version_panel._encoding_panel._encoder_combo.itemText(0) == "Von Merge übernehmen"
        assert dlg._yt_version_panel._encoding_panel._crf_spin.specialValueText() == "Von Merge übernehmen"
        assert dlg._yt_version_panel._encoding_panel._fps_spin.specialValueText() == "Von Merge übernehmen"
        assert dlg._yt_version_panel._encoding_panel._format_combo.itemText(0) == "Von Merge übernehmen"
        assert dlg._yt_version_panel._encoding_panel._resolution_combo.itemText(0) == "Von Merge übernehmen"

    def test_step_encoding_panels_cache_source_material_until_manual_refresh(self, tmp_path):
        source = tmp_path / "source.mp4"
        source.write_text("video", encoding="utf-8")
        job = WorkflowJob(name="Cache", source_mode="files", files=[FileEntry(source_path=str(source))])

        with patch("src.job_workflow.panels.inspector.get_video_stream_info", return_value={"codec_name": "h264", "fps": 25.0, "bit_rate": 1000}) as stream_info, \
             patch("src.job_workflow.panels.inspector.get_resolution", return_value=(1280, 720)), \
             patch("src.job_workflow.panels.inspector.has_audio_stream", return_value=True):
            dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())
            initial_calls = stream_info.call_count

            dlg._refresh_dynamic_sections()
            dlg._refresh_dynamic_sections()

            assert stream_info.call_count == initial_calls

            dlg._merge_encoding_panel._refresh_source_btn.click()

            assert stream_info.call_count > initial_calls

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
        assert all(entry.merge_group_id == "graph-merge" for entry in dlg._draft.files)

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

    def test_pi_source_panel_uses_global_output_root_in_placeholder_and_detail(self):
        settings = _settings()
        settings.workflow_output_root = "/srv/workflows"
        job = WorkflowJob(name="Spieltag 23", source_mode="pi_download", device_name="Pi 1")

        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        assert dlg._pi_dest_edit.text() == ""
        assert dlg._pi_dest_edit.placeholderText() == "/srv/workflows/Pi 1/raw"
        assert "/srv/workflows/Pi 1/raw" in dlg._source_detail_label.text()

    def test_file_and_folder_placeholders_use_workflow_name_plus_date(self):
        settings = _settings()
        settings.workflow_output_root = "/srv/workflows"
        job = WorkflowJob(name="Spieltag 23", source_mode="files")

        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        expected = f"/srv/workflows/Spieltag 23 {date.today().isoformat()}/raw"
        assert dlg._files_dst_edit.placeholderText() == expected
        assert dlg._folder_dst_edit.placeholderText() == expected

    def test_file_placeholders_prefer_merge_camera_when_available(self):
        settings = _settings()
        settings.workflow_output_root = "/srv/workflows"
        job = WorkflowJob(
            name="Spieltag 23",
            source_mode="files",
            merge_segment_data={"camera": "DJI Osmo Action 5 Pro"},
        )

        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        expected = "/srv/workflows/DJI Osmo Action 5 Pro/raw"
        assert dlg._files_dst_edit.placeholderText() == expected
        assert dlg._folder_dst_edit.placeholderText() == expected

    def test_file_placeholders_prefer_youtube_camera_when_no_merge_camera_exists(self):
        settings = _settings()
        settings.workflow_output_root = "/srv/workflows"
        job = WorkflowJob(
            name="Spieltag 23",
            source_mode="files",
            youtube_segment_data={"camera": "DJI Osmo Action 5 Pro"},
        )

        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        expected = "/srv/workflows/DJI Osmo Action 5 Pro/raw"
        assert dlg._files_dst_edit.placeholderText() == expected
        assert dlg._folder_dst_edit.placeholderText() == expected

    def test_selecting_files_node_reapplies_target_placeholder_in_input(self):
        settings = _settings()
        settings.workflow_output_root = "/srv/workflows"
        job = WorkflowJob(name="Spieltag 23", source_mode="files")

        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)
        dlg._files_dst_edit.setPlaceholderText("")

        dlg._on_graph_selection_changed({"kind": "node", "id": "source-files-1", "type": "source_files"})

        assert dlg._files_dst_edit.placeholderText() == f"/srv/workflows/Spieltag 23 {date.today().isoformat()}/raw"

    def test_youtube_metadata_panel_uses_global_settings_defaults_for_blank_job(self):
        settings = _settings()
        settings.default_match_date = "2026-03-20"
        settings.default_match_competition = "Pokal"
        settings.default_match_home_team = "FC Heim"
        settings.default_match_away_team = "FC Gast"

        dlg = JobWorkflowDialog(None, WorkflowJob(), allow_edit=True, settings=settings)

        assert dlg._youtube_metadata_panel._competition_combo.currentText() == ""
        assert dlg._youtube_metadata_panel._home_combo.currentText() == ""
        assert dlg._youtube_metadata_panel._away_combo.currentText() == ""
        assert dlg._youtube_metadata_panel._date_edit.text() == ""
        assert dlg._youtube_metadata_panel._date_edit.placeholderText() == "20.03.2026"
        assert dlg._youtube_metadata_panel._competition_combo.lineEdit().placeholderText() == "Pokal"
        assert dlg._youtube_metadata_panel._home_combo.lineEdit().placeholderText() == "FC Heim"
        assert dlg._youtube_metadata_panel._away_combo.lineEdit().placeholderText() == "FC Gast"
        assert dlg._youtube_metadata_panel._playlist_preview.text() == "20.03.2026 | Pokal | FC Heim vs FC Gast"

    def test_merge_metadata_panel_uses_global_settings_defaults_for_blank_job(self):
        settings = _settings()
        settings.default_match_date = "2026-03-20"
        settings.default_match_competition = "Pokal"
        settings.default_match_home_team = "FC Heim"
        settings.default_match_away_team = "FC Gast"
        settings.default_match_location = "Sportplatz Mitte"

        dlg = JobWorkflowDialog(None, WorkflowJob(), allow_edit=True, settings=settings)

        assert dlg._merge_panel._competition_combo.currentText() == ""
        assert dlg._merge_panel._home_combo.currentText() == ""
        assert dlg._merge_panel._away_combo.currentText() == ""
        assert dlg._merge_panel._location_combo.currentText() == ""
        assert dlg._merge_panel._date_edit.text() == ""
        assert dlg._merge_panel._date_edit.placeholderText() == "20.03.2026"
        assert dlg._merge_panel._competition_combo.lineEdit().placeholderText() == "Pokal"
        assert dlg._merge_panel._location_combo.lineEdit().placeholderText() == "Sportplatz Mitte"
        assert dlg._merge_panel._playlist_preview.text() == "20.03.2026 | Pokal | FC Heim vs FC Gast"

    def test_merge_panel_does_not_persist_placeholder_defaults_as_overrides(self):
        settings = _settings()
        settings.default_match_date = "2026-03-20"
        settings.default_match_competition = "Pokal"
        settings.default_match_home_team = "FC Heim"
        settings.default_match_away_team = "FC Gast"
        settings.default_match_location = "Sportplatz Mitte"

        job = WorkflowJob(files=[FileEntry(source_path="/tmp/a.mp4", merge_group_id="graph-merge")])
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        assert dlg._merge_panel._competition_combo.currentText() == ""
        assert dlg._merge_panel._competition_combo.lineEdit().placeholderText() == "Pokal"

        dlg._apply_and_accept()

        assert job.merge_match_data == {}
        assert job.merge_output_playlist == "20.03.2026 | Pokal | FC Heim vs FC Gast"

    def test_merge_panel_can_clear_explicit_date_override_back_to_general_default(self):
        settings = _settings()
        settings.default_match_date = "2026-03-20"
        settings.default_match_competition = "Pokal"
        settings.default_match_home_team = "FC Heim"
        settings.default_match_away_team = "FC Gast"

        job = WorkflowJob(
            files=[FileEntry(source_path="/tmp/a.mp4", merge_group_id="graph-merge")],
            merge_match_data={"date_iso": "2026-03-22"},
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        assert dlg._merge_panel._date_edit.text() == "22.03.2026"
        dlg._merge_panel._date_edit.setText("")

        assert dlg._merge_panel._date_edit.text() == ""
        assert dlg._merge_panel.current_match_overrides().get("date_iso") is None
        assert dlg._merge_panel._playlist_preview.text() == "20.03.2026 | Pokal | FC Heim vs FC Gast"

        dlg._apply_and_accept()

        assert job.merge_match_data == {}

    def test_merge_panel_date_picker_sets_local_date_override(self):
        settings = _settings()
        settings.default_match_date = "2026-03-20"
        settings.default_match_competition = "Pokal"
        settings.default_match_home_team = "FC Heim"
        settings.default_match_away_team = "FC Gast"

        job = WorkflowJob(files=[FileEntry(source_path="/tmp/a.mp4", merge_group_id="graph-merge")])
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        dlg._merge_panel._date_edit._on_calendar_date_selected(QDate(2026, 3, 22))

        assert dlg._merge_panel._date_edit.text() == "22.03.2026"
        assert dlg._merge_panel.current_match_overrides()["date_iso"] == "2026-03-22"

    def test_youtube_panel_does_not_persist_placeholder_defaults_as_overrides(self):
        settings = _settings()
        settings.default_match_date = "2026-03-20"
        settings.default_match_competition = "Pokal"
        settings.default_match_home_team = "FC Heim"
        settings.default_match_away_team = "FC Gast"
        settings.default_match_location = "Sportplatz Mitte"

        job = WorkflowJob(upload_youtube=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        assert dlg._youtube_metadata_panel._competition_combo.currentText() == ""
        assert dlg._youtube_metadata_panel._competition_combo.lineEdit().placeholderText() == "Pokal"

        dlg._apply_and_accept()

        assert job.youtube_match_data == {}
        assert job.default_youtube_playlist == "20.03.2026 | Pokal | FC Heim vs FC Gast"

    def test_youtube_panel_can_clear_explicit_date_override_back_to_general_default(self):
        settings = _settings()
        settings.default_match_date = "2026-03-20"
        settings.default_match_competition = "Pokal"
        settings.default_match_home_team = "FC Heim"
        settings.default_match_away_team = "FC Gast"

        job = WorkflowJob(
            upload_youtube=True,
            youtube_match_data={"date_iso": "2026-03-22"},
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        assert dlg._youtube_metadata_panel._date_edit.text() == "22.03.2026"
        dlg._youtube_metadata_panel._date_edit.setText("")

        assert dlg._youtube_metadata_panel._date_edit.text() == ""
        assert dlg._youtube_metadata_panel.current_match_overrides().get("date_iso") is None
        assert dlg._youtube_metadata_panel._playlist_preview.text() == "20.03.2026 | Pokal | FC Heim vs FC Gast"

        dlg._apply_and_accept()

        assert job.youtube_match_data == {}

    def test_editor_disables_upload_detail_fields_when_upload_off(self):
        job = _make_job(upload_youtube=True, upload_kaderblick=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        dlg._graph_view.add_node("source_files")

        assert dlg._youtube_metadata_panel.isEnabled() is False
        assert dlg._playlist_helper_btn.isEnabled() is False
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

    def test_graph_rebuild_resets_stale_node_inspector_selection(self):
        job = _make_job(upload_youtube=True, upload_kaderblick=True, convert_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._on_graph_selection_changed({"kind": "node", "type": "kaderblick"})
        assert dlg._property_stack.currentIndex() == dlg._property_pages["kaderblick"]

        dlg._rebuild_graph_from_job()

        assert dlg._property_stack.currentIndex() == dlg._property_pages["default"]
        assert dlg._selection_label.text() == "Keine Auswahl"
        assert dlg._remove_node_btn.isEnabled() is False

    def test_canvas_action_buttons_are_stacked_vertically(self):
        dlg = JobWorkflowDialog(None, _make_job(convert_enabled=True), allow_edit=True, settings=_settings())

        button_layout = dlg._palette_box.layout().itemAt(2).layout()

        assert isinstance(button_layout, QVBoxLayout)
        assert button_layout.itemAt(0).widget() is dlg._remove_node_btn
        assert button_layout.itemAt(1).widget() is dlg._reset_from_node_btn
        assert button_layout.itemAt(2).widget().text() == "Auto-Layout"

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
        assert any("folgen automatisch" in text for text in label_texts)

    def test_output_metadata_preview_uses_api_assignment_label_instead_of_kaderblick(self):
        dlg = JobWorkflowDialog(None, _make_job(upload_youtube=True, upload_kaderblick=True), allow_edit=True, settings=_settings())

        label_texts = {label.text() for label in dlg._merge_panel.findChildren(QLabel)}

        assert "API-Zuordnung:" in label_texts
        assert "Kaderblick:" not in label_texts

    def test_kaderblick_panel_mirrors_youtube_metadata_selection(self):
        dlg = JobWorkflowDialog(None, _make_job(upload_youtube=True, upload_kaderblick=True), allow_edit=True, settings=_settings())

        dlg._kb_video_type_options = [{"id": 3, "name": "Halbzeit"}]
        dlg._kb_camera_options = [{"id": 4, "name": "Hauptkamera"}]
        dlg._sync_kaderblick_selectors()

        dlg._youtube_metadata_panel._camera_combo.setCurrentIndex(max(dlg._youtube_metadata_panel._camera_combo.findData(4), 0))
        dlg._youtube_metadata_panel._video_type_combo.setCurrentIndex(max(dlg._youtube_metadata_panel._video_type_combo.findData(3), 0))

        assert dlg._kb_type_combo.currentData() == 3
        assert dlg._kb_camera_combo.currentData() == 4
        assert dlg._kb_type_combo.isEnabled() is False
        assert dlg._kb_camera_combo.isEnabled() is False

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
        assert dlg._kb_type_combo.currentData() == 2
        assert dlg._kb_camera_combo.currentData() == 1
        assert dlg._kb_type_combo.isEnabled() is False
        assert dlg._kb_camera_combo.isEnabled() is False

    def test_youtube_upload_panel_shows_shared_metadata_read_only_when_merge_output_metadata_is_relevant(self):
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
        assert dlg._youtube_metadata_panel.isEnabled() is False
        assert dlg._playlist_helper_btn.isVisible() is False

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

        assert dlg._youtube_metadata_panel.isEnabled() is False

        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files")
        upload_id = dlg._graph_view.add_node("youtube_upload")
        dlg._graph_view.connect_nodes(source_id, upload_id)

        assert dlg._draft.upload_youtube is True
        assert dlg._youtube_metadata_panel.isEnabled() is True
        assert dlg._playlist_helper_btn.isEnabled() is True

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
        assert dlg._kb_type_combo.isEnabled() is False
        assert dlg._kb_camera_combo.isEnabled() is False

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
        assert dlg._tc_logo_browse_btn.isEnabled() is False
        assert dlg._tc_bg_edit.isEnabled() is False
        assert dlg._tc_fg_edit.isEnabled() is False
        assert dlg._tc_bg_pick_btn.isEnabled() is False
        assert dlg._tc_fg_pick_btn.isEnabled() is False
        assert dlg._tc_preview_frame.isEnabled() is False

    def test_titlecard_color_picker_updates_hex_field(self):
        job = _make_job(title_card_enabled=True, convert_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files")
        title_id = dlg._graph_view.add_node("titlecard")
        dlg._graph_view.connect_nodes(source_id, title_id)

        with patch("src.job_workflow.panels.inspector.QColorDialog.exec", return_value=True), patch(
            "src.job_workflow.panels.inspector.QColorDialog.currentColor", return_value=QColor("#123456")
        ):
            dlg._tc_bg_pick_btn.click()

        assert dlg._tc_bg_edit.text() == "#123456"

    def test_titlecard_preview_updates_from_text_and_colors(self):
        job = WorkflowJob(
            name="Preview Job",
            source_mode="files",
            title_card_enabled=True,
            convert_enabled=True,
            files=[FileEntry(source_path="/tmp/halbzeit_1.mp4", title_card_subtitle="1. Halbzeit")],
        )
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files")
        title_id = dlg._graph_view.add_node("titlecard")
        dlg._graph_view.connect_nodes(source_id, title_id)

        dlg._tc_home_edit.setText("FC Heim")
        dlg._tc_away_edit.setText("FC Gast")
        dlg._tc_date_edit.setText("2026-03-24")
        dlg._tc_bg_edit.setText("#112233")
        dlg._tc_fg_edit.setText("#F5F5F5")
        dlg._tc_duration_spin.setValue(4.0)

        assert dlg._titlecard_panel._tc_preview_frame._title_text == "FC Heim vs FC Gast"
        assert dlg._titlecard_panel._tc_preview_frame._subtitle_text == "1. Halbzeit"
        assert dlg._titlecard_panel._tc_preview_frame._bg_color == "#112233"
        assert dlg._titlecard_panel._tc_preview_frame._fg_color == "#F5F5F5"

    def test_titlecard_logo_picker_updates_path_field(self):
        job = _make_job(title_card_enabled=True, convert_enabled=True)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files")
        title_id = dlg._graph_view.add_node("titlecard")
        dlg._graph_view.connect_nodes(source_id, title_id)

        with patch(
            "src.job_workflow.panels.inspector.QFileDialog.getOpenFileName",
            return_value=("/tmp/logo.png", "Bilder (*.png *.jpg *.jpeg *.svg *.webp)"),
        ):
            dlg._tc_logo_browse_btn.click()

        assert dlg._tc_logo_edit.text() == "/tmp/logo.png"

    def test_graph_change_enables_titlecard_fields_when_titlecard_node_becomes_reachable(self):
        job = _make_job(title_card_enabled=False, convert_enabled=False)
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=_settings())

        assert dlg._tc_bg_pick_btn.isEnabled() is False

        dlg._graph_view.clear_graph()
        source_id = dlg._graph_view.add_node("source_files")
        title_id = dlg._graph_view.add_node("titlecard")
        dlg._graph_view.connect_nodes(source_id, title_id)

        assert dlg._tc_home_edit.isEnabled() is True
        assert dlg._tc_logo_edit.isEnabled() is True
        assert dlg._tc_logo_browse_btn.isEnabled() is True
        assert dlg._tc_bg_pick_btn.isEnabled() is True
        assert dlg._tc_fg_pick_btn.isEnabled() is True
        assert dlg._tc_preview_frame.isEnabled() is True

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

        assert dlg._youtube_metadata_panel._playlist_preview.text() == "22.03.2026 | Kreispokal | FC Heim vs FC Gast"
        assert dlg._youtube_metadata_panel._competition_combo.currentText() == "Kreispokal"
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
        job = WorkflowJob(source_mode="pi_download", device_name="Pi 1", download_destination="/dest")
        dlg = JobWorkflowDialog(None, job, allow_edit=True, settings=settings)

        dlg._device_combo.setCurrentIndex(1)
        dlg._on_camera_files_loaded([
            {"base": "halbzeit1", "total_size": 1_073_741_824},
            {"base": "halbzeit2", "total_size": 536_870_912},
        ])

        assert dlg._pi_file_list.isHidden() is False
        assert dlg._source_panel._pi_selection_label.isHidden() is False
        assert len(dlg._draft.files) == 2
        assert dlg._draft.files[0].source_path.endswith("/dest/halbzeit1.mjpg")
        assert dlg._draft.files[0].source_size_bytes == 1_073_741_824
        assert dlg._draft.files[0].youtube_title == ""
        assert dlg._pi_file_list._table.item(0, dlg._pi_file_list._COL_SIZE).text() == "1.0 GB"