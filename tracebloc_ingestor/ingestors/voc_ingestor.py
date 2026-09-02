"""Pascal-VOC XML record enumerator for object detection (backend#1006).

``object_detection`` required a ``labels.csv`` that carried nothing the VOC XML
did not already have: ``<annotation><filename>`` is the image name and
``<object><name>`` is the class. :class:`VOCIngestor` enumerates records
straight from ``SRC_PATH/annotations/*.xml`` instead, so the user stages images
and annotations and nothing else.

**One record per image.** ``CSVIngestor`` emitted one row per manifest line,
i.e. one row per bounding BOX. This enumerator emits one row per image, which
is the settled record model for backend#1006 and what makes ``record_count``
the image count (dissolving backend#966). It is also what the training side has
assumed all along: the engine samples OD by ``DISTINCT filename`` and reads
``sum(label_counts.values())`` as a desired image count
(``tracebloc-engine/core/utils/database.py``).

A per-image record has no scalar class, so the ``label`` cell holds an encoded
class multiset. Every decision about what that encoding means lives in
:mod:`tracebloc_ingestor.utils.od_label_semantics` — deliberately not here, so
the pending DS call on backend#1006 (box counts vs image-presence counts) is a
one-file change rather than a sweep through this enumerator.
"""

import logging
import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

from .base import BaseIngestor
from ..utils.constants import RED, RESET, YELLOW
from ..utils.od_label_semantics import (
    ODLabelEncodingError,
    distinct_classes,
    encode_image_label,
)

logger = logging.getLogger(__name__)

# Validators that read the record source as a CSV manifest. With no manifest
# there is nothing for them to read, and handing them the annotations DIRECTORY
# makes them fail on a file they were never pointed at. The diversity gate they
# include is not dropped — it is re-derived from the XML in
# ``_validate_label_diversity`` below, which is the only correct source once the
# manifest is gone.
_CSV_READING_VALIDATORS = frozenset(
    {
        "Data Validator",
        "Label Diversity Validator",
        "Duplicate Validator",
        "Ingestable Records",
        "Ingestable Records Validator",
    }
)


class VOCIngestor(BaseIngestor):
    """Enumerate object-detection records from Pascal-VOC XML, one per image.

    ``source`` is the annotations directory (``SRC_PATH/annotations`` by
    default), not a manifest path.
    """

    # One record per image ⇒ unique filenames ⇒ content_hash cannot collapse
    # distinct records, so the manifest-shaped objdet warning in BaseIngestor
    # must not fire for this ingestor.
    enumerates_one_record_per_file = True

    def __init__(
        self, *args: Any, annotations_subdir: str = "annotations", **kwargs: Any
    ):
        super().__init__(*args, **kwargs)
        self.annotations_subdir = annotations_subdir
        # Populated during read_data / validation so the diversity gate and the
        # ingest summary agree on the class set actually enumerated.
        self._seen_label_cells: List[str] = []

    # ------------------------------------------------------------------ paths

    def _annotations_dir(self, source: Any = None) -> Path:
        """Resolve the annotations directory.

        An explicit ``source`` wins UNCONDITIONALLY — including when it does not
        exist. Falling back to ``SRC_PATH`` for a non-existent explicit path
        would report "annotations not found: <SRC_PATH>/annotations" for a
        directory the caller never named, sending them to debug the wrong path;
        ``_xml_files`` instead raises against the path they actually gave.
        Absent a source, use the documented ``SRC_PATH/<annotations_subdir>``
        convention the OD ``ModalitySpec`` already declares as a required
        sidecar.
        """
        if source:
            return Path(str(source))
        return Path(self.database.config.SRC_PATH) / self.annotations_subdir

    def _xml_files(self, source: Any = None) -> List[Path]:
        """Every ``*.xml`` under the annotations directory, in a stable order.

        Sorted so a re-run enumerates identically — with
        ``data_id_strategy="content_hash"`` a retried Job re-claims its rows via
        the data_id UNIQUE upsert, and a stable order keeps the batching and any
        partial-failure reporting reproducible.
        """
        directory = self._annotations_dir(source)
        if not directory.is_dir():
            raise FileNotFoundError(
                f"{RED}Object-detection annotations directory not found: "
                f"{directory}. Stage the Pascal-VOC XML files under "
                f"<SRC_PATH>/{self.annotations_subdir}/ (the chart mounts your "
                f"PVC at /data/shared/).{RESET}"
            )
        return sorted(directory.glob("*.xml"))

    # ------------------------------------------------------------- xml parsing

    @staticmethod
    def _image_filename(root: ET.Element, xml_path: Path) -> str:
        """The image name this annotation belongs to.

        Prefers ``<annotation><filename>``, falling back to the XML's own stem
        (the documented ``{image_name}.xml`` pairing, which ``FilePairingValidator``
        already enforces).

        The XML is user-supplied, so the value is reduced to its basename before
        it is ever used to build a path. ``file_transfer._safe_join`` rejects
        traversal independently (data-ingestors#239, already fixed and closed) —
        this is the enumerator not manufacturing a hostile value in the first
        place, not a replacement for that guard.
        """
        node = root.find("filename")
        raw = (node.text or "").strip() if node is not None else ""
        if not raw:
            return xml_path.stem
        basename = os.path.basename(raw.replace("\\", "/")).strip()
        if not basename or basename in {".", ".."}:
            return xml_path.stem
        return basename

    @staticmethod
    def _object_classes(root: ET.Element) -> List[str]:
        """Every ``<object><name>`` in document order (one entry per box)."""
        names = []
        for obj in root.findall(".//object"):
            node = obj.find("name")
            if node is not None and (node.text or "").strip():
                names.append(node.text.strip())
        return names

    def _record_from_xml(self, xml_path: Path) -> Optional[Dict[str, Any]]:
        """Parse one annotation into a record, or ``None`` to skip it.

        A malformed or object-less XML is skipped with a warning rather than
        aborting the run: ``PascalVOCXMLValidator`` runs before ingest and is the
        component that fails a genuinely broken annotation set. An encoding error
        is NOT skipped — it means a class name cannot survive the label round
        trip, which would silently corrupt the histogram, so it propagates.
        """
        try:
            root = ET.parse(xml_path).getroot()
        except ET.ParseError as exc:
            logger.warning(
                f"{YELLOW}Skipping unparseable annotation {xml_path.name}: {exc}{RESET}"
            )
            return None

        classes = self._object_classes(root)
        if not classes:
            logger.warning(
                f"{YELLOW}Skipping {xml_path.name}: no <object><name> entries — "
                f"an image with no annotated objects contributes no class "
                f"information.{RESET}"
            )
            return None

        return {
            "filename": self._image_filename(root, xml_path),
            self.label_column or "label": encode_image_label(classes),
        }

    # --------------------------------------------------------- base overrides

    def read_data(self, source: Any) -> Generator[Dict[str, Any], None, None]:
        """Yield one record per annotated image."""
        self._seen_label_cells = []
        for xml_path in self._xml_files(source):
            record = self._record_from_xml(xml_path)
            if record is None:
                continue
            self._seen_label_cells.append(record[self.label_column or "label"])
            yield record

    def _count_records(self, source: Any) -> Optional[int]:
        """Number of annotation files — the upper bound on enumerated images.

        Progress-reporting only (skipped files make the real total smaller), so
        an unreadable directory returns ``None`` rather than raising here; the
        read path raises with the actionable message.
        """
        try:
            return len(self._xml_files(source))
        except Exception as exc:
            logger.debug(f"{YELLOW}Unable to count VOC annotations: {exc}{RESET}")
            return None

    def validate_data(self, source: Any) -> bool:
        """Run the object-detection validators, minus the CSV-manifest readers.

        The OD validator set is already almost entirely directory-driven —
        ``FileTypeValidator`` (images / annotations), ``PascalVOCXMLValidator``,
        ``FilePairingValidator``, ``ImageResolutionValidator`` all read paths
        from config and ignore ``source``. Only the centrally-injected
        CSV-reading validators need replacing, and the one that carries real
        semantics — the >= 2 distinct class gate — is re-derived from the XML.
        """
        from ..ingestors import preflight
        from ..utils.validators_mapping import map_validators

        preflight.check_src_path(self.database.config)

        validators = [
            validator
            for validator in map_validators(
                self.category,
                {**self.file_options, "label_column": self.label_column},
                self.database.config,
            )
            if validator.name not in _CSV_READING_VALIDATORS
        ]
        self._run_validators(validators, source)
        self._validate_label_diversity(source)
        return True

    def _validate_label_diversity(self, source: Any) -> None:
        """Re-derive the >= 2 distinct class gate from the annotations.

        ``LabelDiversityValidator`` counts distinct values of the CSV label
        column. Pointing it at the per-image cells would be wrong twice over:
        there is no CSV, and a cell is a whole class MULTISET, so a dataset of
        cars-and-signs images would look like a single distinct value and be
        false-rejected. The gate's real question — does this dataset carry at
        least two classes — is answered by the distinct classes across all
        annotations.
        """
        cells = self._seen_label_cells or [
            record[self.label_column or "label"]
            for record in (
                self._record_from_xml(path) for path in self._xml_files(source)
            )
            if record is not None
        ]
        classes = distinct_classes(cells)
        if len(classes) < 2:
            raise ValueError(
                f"{RED}Object-detection dataset has {len(classes)} distinct "
                f"class(es) ({classes or 'none'}) across its Pascal-VOC "
                f"annotations; at least 2 are required. Check "
                f"<object><name> in <SRC_PATH>/{self.annotations_subdir}/."
                f"{RESET}"
            )

    def ingest(self, source: Any = None, batch_size: int = 50) -> List[Dict[str, Any]]:
        """Ingest from the annotations directory, with VOC-specific logging."""
        directory = self._annotations_dir(source)
        logger.info(f"Starting Pascal-VOC ingestion from {directory}")
        failed_records = super().ingest(str(directory), batch_size)
        logger.info(
            f"Pascal-VOC ingestion completed. Failed records: {len(failed_records)}"
        )
        return failed_records


__all__ = ["VOCIngestor", "ODLabelEncodingError"]
