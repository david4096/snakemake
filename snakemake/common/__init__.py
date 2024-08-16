__author__ = "Johannes Köster"
__copyright__ = "Copyright 2023, Johannes Köster"
__email__ = "johannes.koester@protonmail.com"
__license__ = "MIT"

import contextlib
import itertools
import math
import operator
import platform
import hashlib
import inspect
import sys
import uuid
import os
import asyncio
import collections
from pathlib import Path

from snakemake._version import get_versions

from snakemake_interface_common.exceptions import WorkflowError

__version__ = get_versions()["version"]
del get_versions


MIN_PY_VERSION = (3, 7)
UUID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://snakemake.readthedocs.io")
NOTHING_TO_BE_DONE_MSG = (
    "Nothing to be done (all requested files are present and up to date)."
)

ON_WINDOWS = platform.system() == "Windows"
# limit the number of input/output files list in job properties
# see https://github.com/snakemake/snakemake/issues/2097
IO_PROP_LIMIT = 100
SNAKEFILE_CHOICES = list(
    map(
        Path,
        (
            "Snakefile",
            "snakefile",
            "workflow/Snakefile",
            "workflow/snakefile",
        ),
    )
)
PIP_DEPLOYMENTS_PATH = ".snakemake/pip-deployments"


def get_snakemake_searchpaths():
    """Retrieve a list of search paths for Snakemake."""
    paths = [str(Path(__file__).parent.parent.parent)] + [
        path for path in sys.path if os.path.isdir(path)
    ]
    return list(unique_justseen(paths))


def mb_to_mib(mb):
    """Convert megabytes (MB) to mebibytes (MiB).

    Args:
        mb (int): The size in megabytes.

    Returns:
        int: The size in mebibytes, rounded up.
    """
    return int(math.ceil(mb * 0.95367431640625))


def parse_key_value_arg(arg, errmsg, strip_quotes=True):
    """Parse a key-value pair from a string argument.

    Args:
        arg (str): The argument string to parse, formatted as 'key=value'.
        errmsg (str): The error message to raise if parsing fails.
        strip_quotes (bool, optional): Whether to strip quotes from the value. Defaults to True.

    Returns:
        tuple: A tuple containing the key and value as strings.
    """
    try:
        key, val = arg.split("=", 1)
    except ValueError:
        raise ValueError(errmsg + f" (Unparsable value: {repr(arg)})")
    if strip_quotes:
        val = val.strip("'\"")
    return key, val


def dict_to_key_value_args(
    some_dict: dict, quote_str: bool = True, repr_obj: bool = False
):
    """Convert a dictionary to a list of key-value pair strings.

    Args:
        some_dict (dict): The dictionary to convert.
        quote_str (bool, optional): Whether to quote string values. Defaults to True.
        repr_obj (bool, optional): Whether to represent objects using repr. Defaults to False.

    Returns:
        list: A list of key-value pair strings.
    """
    items = []
    for key, value in some_dict.items():
        if repr_obj and not isinstance(value, str):
            encoded = repr(value)
        else:
            encoded = f"'{value}'" if quote_str and isinstance(value, str) else value
        items.append(f"{key}={encoded}")
    return items


def async_run(coroutine):
    """Attaches to running event loop or creates a new one to execute a
    coroutine.
    .. seealso::
         https://github.com/snakemake/snakemake/issues/1105
         https://stackoverflow.com/a/65696398

    Args:
        coroutine (coroutine): The coroutine to run.

    Returns:
        Any: The result of the coroutine execution.

    Raises:
        WorkflowError: If Snakemake is run from an already running event loop.
    """
    try:
        return asyncio.run(coroutine)
    except RuntimeError as e:
        coroutine.close()
        raise WorkflowError(
            "Error running coroutine in event loop. Snakemake currently does not "
            "support being executed from an already running event loop. "
            "If you run Snakemake e.g. from a Jupyter notebook, make sure to spawn a "
            "separate process for Snakemake.",
            e,
        )


APPDIRS = None


RULEFUNC_CONTEXT_MARKER = "__is_snakemake_rule_func"


def get_appdirs():
    """Get the application directories for Snakemake.

    Returns:
        AppDirs: The application directories.
    """
    global APPDIRS
    if APPDIRS is None:
        from appdirs import AppDirs

        APPDIRS = AppDirs("snakemake", "snakemake")
    return APPDIRS


def is_local_file(path_or_uri):
    """Check if a path or URI is a local file.

    Args:
        path_or_uri (str): The path or URI to check.

    Returns:
        bool: True if the path or URI is a local file, False otherwise.
    """
    return parse_uri(path_or_uri).scheme == "file"


def parse_uri(path_or_uri):
    """Parse a URI or file path.

    Args:
        path_or_uri (str): The URI or file path to parse.

    Returns:
        Uri: A named tuple with scheme and uri_path attributes.

    Raises:
        NotImplementedError: If the URI scheme is not supported by smart_open.
    """
    from smart_open import parse_uri

    try:
        return parse_uri(path_or_uri)
    except NotImplementedError as e:
        # Snakemake sees a lot of URIs which are not supported by smart_open yet
        # "docker", "git+file", "shub", "ncbi","root","roots","rootk", "gsiftp",
        # "srm","ega","ab","dropbox"
        # Fall back to a simple split if we encounter something which isn't supported.
        scheme, _, uri_path = path_or_uri.partition("://")
        if scheme and uri_path:
            uri = collections.namedtuple("Uri", ["scheme", "uri_path"])
            return uri(scheme, uri_path)
        else:
            raise e


def smart_join(base, path, abspath=False):
    """Join a base path or URI with a relative path.

    Args:
        base (str): The base path or URI.
        path (str): The relative path to join.
        abspath (bool, optional): Whether to return an absolute path. Defaults to False.

    Returns:
        str: The joined path or URI.
    """
    if is_local_file(base):
        full = os.path.join(base, path)
        if abspath:
            return os.path.abspath(full)
        return full
    else:
        from smart_open import parse_uri

        uri = parse_uri(f"{base}/{path}")
        if not ON_WINDOWS:
            # Norm the path such that it does not contain any ../,
            # which is invalid in an URL.
            assert uri.uri_path[0] == "/"
            uri_path = os.path.normpath(uri.uri_path)
        else:
            uri_path = uri.uri_path
        return f"{uri.scheme}:/{uri_path}"


def num_if_possible(s):
    """Convert a string to a number if possible, otherwise return the string.

    Args:
        s (str): The string to convert.

    Returns:
        int, float, or str: The converted number or the original string.
    """
    try:
        return int(s)
    except ValueError:
        try:
            return float(s)
        except ValueError:
            return s


def get_last_stable_version():
    """Get the last stable version of Snakemake.

    Returns:
        str: The last stable version string.
    """
    return __version__.split("+")[0]


def get_container_image():
    """Get the Snakemake container image string.

    Returns:
        str: The container image string.
    """
    return f"snakemake/snakemake:v{get_last_stable_version()}"


def get_uuid(name):
    """Generate a UUID for a given name using the Snakemake namespace.

    Args:
        name (str): The name to generate a UUID for.

    Returns:
        UUID: The generated UUID.
    """
    return uuid.uuid5(UUID_NAMESPACE, name)


def get_file_hash(filename, algorithm="sha256"):
    """Find the SHA256 hash string of a file. We use this so that the
-    user can choose to cache working directories in storage.

    Args:
        filename (str): The path to the file.
        algorithm (str, optional): The hash algorithm to use (default: "sha256").

    Returns:
        str: The hash string.

    Raises:
        ValueError: If the algorithm is not available.
    """
    from snakemake.logging import logger

    # The algorithm must be available
    try:
        hasher = hashlib.new(algorithm)
    except ValueError as ex:
        logger.error("%s is not an available algorithm." % algorithm)
        raise ex

    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def bytesto(bytes, to, bsize=1024):
    """Convert bytes to megabytes.
    bytes to mb: bytesto(bytes, 'm')
    bytes to gb: bytesto(bytes, 'g' etc.
    From https://gist.github.com/shawnbutts/3906915

    Args:
        bytes (int): The number of bytes.
        to (str): The target unit (e.g., 'k', 'm', 'g').
        bsize (int, optional): The block size for conversion. Defaults to 1024.

    Returns:
        float: The converted value.
    """
    levels = {"k": 1, "m": 2, "g": 3, "t": 4, "p": 5, "e": 6}
    answer = float(bytes)
    for _ in range(levels[to]):
        answer = answer / bsize
    return answer


def is_rule_or_checkpoint(func):
    """Check if a function is a Snakemake rule or checkpoint.

    Args:
        func (function): The function to check.

    Returns:
        bool: True if the function is a rule or checkpoint, False otherwise.
    """
    return getattr(func, RULEFUNC_CONTEXT_MARKER, False)


@contextlib.contextmanager
def capture():
    """Capture stdout and stderr during the context."""
    import io
    from snakemake.logging import logger

    oldout, olderr = sys.stdout, sys.stderr
    try:
        out = [io.StringIO(), io.StringIO()]
        sys.stdout, sys.stderr = out
        yield out
    except Exception as e:
        logger.error(f"Error during capture: {e}")
        raise e
    finally:
        sys.stdout, sys.stderr = oldout, olderr


def unique_justseen(iterable, key=None):
    """List unique elements, preserving order. Remember only the element just seen.

    Args:
        iterable (iterable): The input iterable.
        key (function, optional): A function to compute the key value for each element.

    Returns:
        iterator: An iterator of unique elements.
    """
    return map(next, map(operator.itemgetter(1), itertools.groupby(iterable, key)))


def split_filename(path, basedir=None):
    """Split a path into its base directory and the filename.

    Args:
        path (str): The path to split.
        basedir (str, optional): The base directory. If None, the base directory of the path is used.

    Returns:
        tuple: A tuple containing the base directory and filename.
    """
    if basedir:
        full = Path(basedir) / path
    else:
        full = Path(path)
    return full.parent, full.name


def sanitize_wildcard(wildcard):
    """Sanitize a wildcard for safe use in file names.

    Args:
        wildcard (str): The wildcard string to sanitize.

    Returns:
        str: The sanitized wildcard string.
    """
    return wildcard.replace(",", "__").replace(":", "__")


def check_for_potential_iostream_unclosed_warnings():
    """Check for potential unclosed iostream warnings in the logging module."""
    from snakemake.logging import logger

    if ON_WINDOWS:
        try:
            import __main__

            for objname in dir(__main__):
                obj = getattr(__main__, objname)
                if isinstance(obj, (asyncio.StreamReader, asyncio.StreamWriter)):
                    logger.warning(
                        "Potentially unclosed iostream detected in "
                        f"the logging module: {objname}."
                    )
        except ImportError as e:
            logger.error(f"Error importing module: {e}")
        raise e


def format_wildcards_as_tokens(wildcards):
    """Format wildcards as tokens for use in filenames or paths.

    Args:
        wildcards (dict): The wildcards to format.

    Returns:
        dict: The formatted wildcards.
    """
    return {key: sanitize_wildcard(str(value)) for key, value in wildcards.items()}


def get_mem_mb(mem):
    """Convert memory string (e.g., '2G') to megabytes.

    Args:
        mem (str): The memory string to convert.

    Returns:
        int: The memory in megabytes.

    Raises:
        ValueError: If the memory string is not in the correct format.
    """
    num = int(mem[:-1])
    unit = mem[-1].lower()
    if unit == "g":
        return num * 1024
    elif unit == "m":
        return num
    elif unit == "k":
        return num // 1024
    else:
        raise ValueError("Unsupported memory unit: %s" % unit)


def join_with_spaces(*args):
    """Join arguments with spaces.

    Args:
        *args: Arguments to join.

    Returns:
        str: The joined string.
    """
    return " ".join(map(str, args))


def get_io_prop_string(input_list, limit=IO_PROP_LIMIT):
    """Create a string of input/output properties for job properties.

    Args:
        input_list (list): The list of input/output files.
        limit (int, optional): The maximum number of files to include in the string. Defaults to IO_PROP_LIMIT.

    Returns:
        str: A string of input/output properties.
    """
    if len(input_list) > limit:
        input_list = input_list[:limit] + ["..."]
    return ", ".join(input_list)


def uuid_to_short_id(uuid_str):
    """Convert a UUID to a short identifier.

    Args:
        uuid_str (str): The UUID string.

    Returns:
        str: The short identifier.
    """
    return uuid_str.split("-")[0]


def compile_path_glob(pathtype, pattern):
    """Compile a glob pattern into a regex for file matching.

    Args:
        pathtype (str): The type of path ('input' or 'output').
        pattern (str): The glob pattern to compile.

    Returns:
        re.Pattern: The compiled regex pattern.
    """
    import re

    if pathtype not in {"input", "output"}:
        raise ValueError("Invalid path type: %s" % pathtype)
    pattern = pattern.replace(".", r"\.").replace("*", r"[^/]*").replace("?", r"[^/]")
    if pathtype == "input":
        pattern = pattern.replace("[^/]*", ".*")
    pattern = "^" + pattern + "$"
    return re.compile(pattern)


def join_with_trailing_slash(path):
    """Join a path with a trailing slash.

    Args:
        path (str): The path to join.

    Returns:
        str: The joined path with a trailing slash.
    """
    return os.path.join(path, "")


def check_circular_dependency(jobs):
    """Check for circular dependencies among jobs.

    Args:
        jobs (list): The list of jobs to check.

    Raises:
        WorkflowError: If a circular dependency is found.
    """
    from snakemake.logging import logger

    visited = set()

    def visit(job):
        if job in visited:
            return
        visited.add(job)
        for dep in job.dependencies:
            visit(dep)
            if dep in job.dependencies:
                logger.error("Circular dependency found: %s" % job)
                raise WorkflowError("Circular dependency detected")

    for job in jobs:
        visit(job)
