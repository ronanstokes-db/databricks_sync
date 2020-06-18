#!/usr/bin/env python3

#imports
import argparse
import os
import json
import logging
import glob
import subprocess
import re
import sys
from operator import itemgetter
import fnmatch

# typical workflow:
#
#


class DatabricksSync:
    """ Class to implement Git sync with workspace """

    def __init__(self):
        self.commands_to_execute=[]
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.WARNING)
        self.logger = logging.getLogger("DatabricksSync")
        self.program="databricks_sync"
        self.config={ "dummy":"test" }

    def add_std_options(self, parser, group="Options"):
        group=parser.add_argument_group(group)
        group.add_argument( "--verbose", "-v", help="Increase output verbosity",
                            action="store_true")
        group.add_argument("--dry-run",  help="Show what would be executed without executing",
                            action="store_true", dest="dryrun")
        group.add_argument("--debug","-d",  help="Debug mode. Show more detail and full stack trace on error.",
                            action="store_true")
        group.add_argument("--profile", help="Profile to use when connecting to Databricks workspace"
                            )
        group.add_argument("--help","-h",  help="Display help and exit.",
                            action="help")

        #group.add_argument("--help","-h",  help="Display help and exit"
        #                    )
        return group


    def parse_args(self):
        """ Parse args and set up instance properties

        This sets up the commands and their options
        """

        description_text="""
        This program allows synchronization of a Databricks workspace with local git / enterprise GitHub repositories
        """

        epilog_text="""
        The command `%(prog)s` provides interim support for synchronizing Databricks workspace objects
        with Git and Enterprise Github.
        
        It will be superceded by future Databricks Workspace repository and CICD features.
        """

        recursive_prompt="""
        scan dirs recursively for match. 
        If any path contains `**` this is implied.
        """

        path_epilog="""
        Paths can contain git style wildcards such as `**/*.py`. If the Git style directory wildcard `**` is used
        for any path, recursive is implied.
        
        If path is not absolute, the default root path is preprended to the path.
        """

        # create top level arg parser
        parser = argparse.ArgumentParser(prog=self.program,
                                         description=description_text,
                                         epilog=epilog_text,
                                         usage="%(prog)s [OPTIONS] COMMAND [COMMAND-OPTIONS] [ARGS]",
                                         conflict_handler='resolve',
                                         add_help=False)
        self.add_std_options(parser)

        # create sub command parsers
        subparsers = parser.add_subparsers(title="Commands", description="one of the following commands:",
                                           dest='command', help="Sub-command help")



        parser_ls = subparsers.add_parser('ls',
                                          help="List contents of Databricks workspace folder",
                                          description="List contents of Databricks workspace folder location",
                                          usage="{} ls [-d] [-v] [-l] [-R] [--absolute] path".format(self.program),
                                          conflict_handler='resolve', add_help=False,
                                          epilog=path_epilog,
                                          prog="Command [databricks_sync ls]"
                                          )
        group_args = parser_ls.add_argument_group("Arguments")
        group_args_options=self.add_std_options(parser_ls, "Command Options")
        group_args_options.add_argument("-l", "--long", help="list objects in long form",
                                        action="store_true")
        group_args_options.add_argument("--absolute", help="list objects using absolute paths",
                                        action="store_true")
        group_args_options.add_argument("-R", "--recursive", help=recursive_prompt,
                                        action="store_true")
        group_args.add_argument("path", help="list objects from workspace path `path`")

        parser_diff = subparsers.add_parser('diff', help="Show differences between local filesystem and Databricks workspace folder",
                                          usage="{} diff [COMMAND-OPTIONS] src_path wksp_path".format(self.program),
                                          description="Show file level differences between local file system and databricks workspace folder location",
                                          conflict_handler='resolve', add_help=False,
                                          epilog="Note: It does not compare file contents when a file exists in both databricks workspace and local file system",
                                          prog="Command [databricks_sync diff]"
                                          )
        diff_args = parser_diff.add_argument_group("Arguments")
        group_args_options=self.add_std_options(parser_diff, "Command Options")
        group_args_options.add_argument("-l", "--long", help="list objects in long form",
                                        action="store_true")
        group_args_options.add_argument("--absolute", help="list objects using absolute paths",
                                        action="store_true")
        group_args_options.add_argument("-R", "--recursive", help=recursive_prompt,
                                        action="store_true")
        diff_args.add_argument("src_path", help="local path for comparison")
        diff_args.add_argument("wksp_path", help="workspace path for comparison")


        parser_export = subparsers.add_parser('export', help="Export notebooks  from Databricks workspace",
                                            usage="{} export [COMMAND-OPTIONS] src-path tgt-path".format(self.program),
                                            description="Export one or more notebooks from a databricks workspace",
                                            conflict_handler='resolve', add_help=False,
                                            epilog="Note: non-notebook files are ignored")
        group_args2 = parser_export.add_argument_group("Arguments")
        group_export=self.add_std_options(parser_export, "Command Options")

        group_args2.add_argument("wksp_path", help="Workspace path to get files from")
        group_args2.add_argument("tgt_path", help="local path to place files in")

        group_export.add_argument("-o", "--overwrite", help="overwrite local files if they exist",
                                 action="store_true")
        group_export.add_argument("--format", help="format to use when downloading the files",
                                 choices=["SOURCE", "DBC", "JUPYTER", "HTML", "source", "dbc", "jupyter", "html"],
                                  required=True)
        group_export.add_argument("-R", "--recursive", help=recursive_prompt,
                                 action="store_true")
        group_export.add_argument( "--no-commit", help="Don't commit changes to local git",
                                  action="store_true", default=False)
        group_export.add_argument( "--push-to",
                                   help="Push changes to remote github. Use form : `--push-to remote/branch`",
                                  )


        #parser_pull.set_defaults(func=self.pull)

        import_epilog="""
        For example :
          databricks_sync import -l PYTHON --format SOURCE *.py "TestSync"
        """
        parser_import = subparsers.add_parser('import', help="Import notebooks to Databricks workspace",
                                            usage="{} import [COMMAND-OPTIONS] src_path tgt_path".format(self.program),
                                            conflict_handler='resolve', add_help=False,
                                              epilog=import_epilog, prog="Command [databricks_sync import]"

                                              )
        group_args3 = parser_import.add_argument_group("Arguments")
        group_import=self.add_std_options(parser_import, "Command Options")
        group_args3.add_argument("src_path", help="local path to take notebook files from ")
        group_args3.add_argument("wksp_path", help="target workspace path")
        group_import.add_argument("-o", "--overwrite", help="overwrite local files if they exist",
                                 action="store_true")
        group_import.add_argument("-f", "--force", help="force changes even if otherwise warnings or errors flagged",
                                 action="store_true")
        group_import.add_argument("-k", "--keep-extensions",
                                  help="keep source extensions when importing ",
                                 action="store_true", default=False)
        group_import.add_argument("-R", "--recursive", help=recursive_prompt,
                                 action="store_true")
        group_import.add_argument("-l", "--language", help="base language for notebook",
                                 choices=["SCALA", "PYTHON", "SQL", "R", "scala", "python", "sql", "r"],
                                  required=True)

        group_import.add_argument("--format", help="format to use when downloading the files",
                                 choices=["SOURCE", "DBC", "JUPYTER", "HTML", "source", "dbc", "jupyter", "html"],
                                  default="SOURCE")


        #parser_push.set_defaults(func=self.pull)

        parser_config = subparsers.add_parser('configure', help="Configure defaults for subsequent commands",
                                              description="Configure default settings for subsequent commands",
                                              usage="{} configure [COMMAND-OPTIONS] ".format(self.program),
                                              conflict_handler='resolve', add_help=False)
        group_config=self.add_std_options(parser_config, "Command Options")
        #parser_config.set_defaults(func=self.configure)

        args = parser.parse_args()

        if args.verbose:
            self.logger.setLevel(logging.INFO)
            self.logger.info("setting log level to INFO")
        if args.debug:
            self.logger.setLevel(logging.DEBUG)
            self.logger.debug("setting log level to DEBUG")

        self.logger.debug("args: %s", str(args))

        return parser, args

    def add_command(self, cmd):
        """ add command to set of commands to execute"""
        assert cmd is not None
        self.logger.debug("adding command : %s", str(cmd))
        self.commands_to_execute.append(cmd)

    def execute_cmd_ex(self, cmd):
        """ Execute a single command """
        assert type(cmd) is list, "Command must be list"
        self.logger.info("Executing command [{}]".format(str(cmd)))
        exit_status = subprocess.run(cmd,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     universal_newlines=True
                                     )

        cmd_output=str(exit_status.stdout).split('\n')
        self.logger.debug("Exit status is : %d", exit_status.returncode)
        return (exit_status, cmd_output)


    def execute_cmds_ex(self, args):
        """ Execute a group of commands """
        if args.dryrun:
            for cmd in self.commands_to_execute:
                print("Dryrun: Executing command [{}]".format(cmd))
        else:
            for cmd in self.commands_to_execute:
                cmd_stat, cmd_out = self.execute_cmd_ex(cmd)
                if cmd_stat.returncode != 0:
                    self.logger.error("Error executing command : %s", cmd_out)
                    raise RuntimeError("Failure executing command : %s", cmd)

    def get_modified_or_untracked_changes(self, filepath, recursive=False, modified_only=False):
        """Gets the sets of files in the current directory or lower that have been modiifed
        since last checkin or are untracked"""

        git_status,git_status_out = self.execute_cmd_ex(["git", "status", "-s", "-u", "normal", filepath])

        if git_status.returncode != 0:
            raise RuntimeError("git status failed")

        # get the git status where if impacts files in current or lower directories
        modified_files = [ y
                           for y in [ (x[:2], x[2:].strip()) for x in list(git_status_out)]
                           if not y[1].startswith("..") and len(y[1]) > 0]

        # by default, git status is recursive
        if modified_only:
            modified_files = list(filter(lambda x: "??" != x[0], modified_files))

        if recursive:
            return modified_files
        else:
            return list(filter(lambda x: "/" not  in x[1], modified_files))

    def mk_local_file_from_notebook(self, path, language, format):
        """ Determine extension based on path, language and format"""
        language_mappings = { "R" : '.r', "PYTHON" : ".py", "SCALA" : ".scala"}
        output = path
        if format == "DBC":
            output= path + ".dbc"
        elif format == "HTML":
            output= path + ".html"
        elif format == "SOURCE" or format=="JUPYTER":
            output= path + language_mappings[language]
        return output.replace("(", "_").replace(")", "_")

    def escaped_file(self, path):
        """ Escape a filename
        :param path: file name to escape
        :return: escaped filename
        """
        return (path.replace(" ", r"\ ")
                .replace("?", r"\\?")
                .replace("[", r"\\[")
                .replace("]", r"\\]")
                .replace("(", r"\\(")
                .replace(")", r"\\)")
                .replace("*", r"\\*")
                )

    def export_from_workspace(self, args):
        """ Exports will grab notebooks from remote databricks workspace and
            check them into local git

            It will fail if there are untracked or uncommitted changes in the local environment
        """
        """Export command implementation"""
        self.logger.debug("starting export")
        self.get_params(args)

        remote = ["", ""]
        if args.push_to is not None:
            assert args.no_commit == False, "Cannot have option --no-commit with option --push-to"
            remote=[ x.strip() for x in args.push_to.split("/") ]
            assert remote is not None and len(remote) == 2, "must have remote of the form `remote/branch`"
            assert len(remote[0]) > 0, "must have remote of the form `remote/branch`"
            assert len(remote[1]) > 0, "must have remote of the form `remote/branch`"

        #  get list of files matching pattern

        if "**" in args.tgt_path or "**" in args.wksp_path:
            args.recursive=True

        args.absolute=False

        self.logger.info("checking for uncommitted changes ")

        # get `git status -s` output for current directory
        # filter files from both listings with `..` paths
        modified_files = self.get_modified_or_untracked_changes(args.tgt_path, recursive=args.recursive,
                                                                modified_only=True)
        self.logger.debug("Uncommited changes : %s", modified_files)

        # check if any files that would be imported to workspace have uncommitted or untracked changes
        # if so, exit with error , unless `--force` was specified
        if modified_files is not None and len(modified_files) > 0:
            self.logger.error("There are modified files not checked in to local repo:\n  %s",
                                str(modified_files))
            raise RuntimeError("There are uncommitted changes : {}".format(str(modified_files)))

        # determine files to export from workspace
        basePath, baseName= os.path.split(args.wksp_path)
        match_pattern=""

        if self.has_magic(baseName):
            match_pattern=baseName
            effective_path = self.mk_workspace_path(basePath)
            self.logger.info("Retrieving workspace files path [%s] pattern [%s]",
                              effective_path, match_pattern)
        else:
            effective_path = self.mk_workspace_path(args.wksp_path)
            self.logger.info("Retrieving workspace files path [%s]", effective_path)


        wksp_contents = self.get_workspace_listing(effective_path, extended=True,
                                                   absolute_paths=False,
                                                   recursive=args.recursive,
                                                   showProgress=False,
                                                   omit_dirs=True)

        # Get the set of folders that need to be created
        new_folders = set([y for y in  [ os.path.dirname(x[2]) for x in wksp_contents]
                       if y is not None and len(y) > 0])

        # ... and add commands to make them
        for new_folder in new_folders:
            cmd=[ 'mkdir', '-p', new_folder ]
            self.add_command(cmd)
        self.logger.info("folders to create: %s", new_folders)

        # get set of files to export
        for x in wksp_contents:
            src_path=x[2]
            tgt_file = self.mk_local_file_from_notebook(x[2], x[3], args.format)
            print(" +++ {}".format(tgt_file))

            if os.path.exists(tgt_file) and not args.overwrite:
                self.logger.error("File exists [%s] - specify `--overwrite` to overwrite it", tgt_file)
                raise RuntimeError("Export would replace existing file and `--overwrite` was not specified")

            # add command to export notebook to file
            cmd = ['databricks', 'workspace', 'export' , '--profile', self.profile_to_use ]

            if args.overwrite:
                cmd.append("--overwrite")
            cmd.extend([ "--format", args.format,
                         self.mk_workspace_path(effective_path, src_path),
                         tgt_file])

            self.add_command(cmd)

            # add command to add file
            cmd = ['git', 'add',  """{}""".format(tgt_file) ]
            self.add_command(cmd)


        # add command for commit
        if not args.no_commit:
            cmd = ['git', 'commit', '-m', """'commited changes exported from workspace'"""]
            self.add_command(cmd)

        # add command for push to

        if args.push_to is not None:
            cmd = ['git', 'push', remote[0], remote[1]]
            self.add_command(cmd)

        self.execute_cmds_ex(args)

        return



    magic_check=re.compile("([?*[])")

    def has_magic(self, s):
        """check if s has wild cards"""
        match=self.magic_check.search(s)
        return match is not None

    def get_dir_listing_ex(self, filepath, recursive=False):
        """ Get listing of local file system"""
        # files = os.scandir(filepath)
        # return [ x.path for x in files]
        self.logger.debug("getting file listing for [%s] with recursive: %s", filepath, recursive)
        return glob.glob(filepath, recursive=recursive)

    def get_dir_listing(self, filepath, recursive=False):
        """ Get listing of local file system"""
        #files = os.scandir(filepath)
        #return [ x.path for x in files]
        self.logger.debug("getting file listing for [%s] with recursive: %s", filepath, recursive)
        return glob.glob(filepath, recursive=recursive)

    def _wksp_folder_listing(self, filePath, extended, absolute_paths):
        """Get workspace folder listing for single folder"""
        cmd = ['databricks', 'workspace', 'ls', '--profile', self.profile_to_use]
        if extended:
            cmd.append("-l")

        if absolute_paths:
            cmd.append("--absolute")

        cmd.append(filePath)
        git_status, git_status_out = self.execute_cmd_ex(cmd)
        return git_status, git_status_out

    def match_filter(self, filename, filter):
        """ Match file name or notebook name against filter """
        if filter is None:
            return True

        dirn, basen = os.path.split(filename)
        return fnmatch.fnmatch(basen, filter)


    def get_workspace_listing(self, filepath, extended=False, absolute_paths=False, recursive=False,
                              allow_other=False, omit_dirs=False,
                              showProgress=False):
        """ Get listing of workspace at path"""
        effective_path = self.mk_workspace_path(filepath)
        self.logger.debug("effective path : %s", effective_path)

        pattern_filter=None

        if self.has_magic(effective_path):
            effective_path, pattern_filter = os.path.split(effective_path)

        re_notebook=re.compile("^NOTEBOOK +(.*) +([A-Z]+)$")
        re_folder=re.compile("^DIRECTORY +(.*)$")
        re_other = re.compile("^([A-Z]+) +(.*)$")



        folders_to_process = [ effective_path ]
        files = []
        if showProgress:
            sys.stdout.write("Getting workspace listing ...")
            sys.stdout.flush()

        # process folders
        while len(folders_to_process) > 0:
            if showProgress:
                sys.stdout.write(".")
                sys.stdout.flush()
            path_to_process = folders_to_process[0]
            folders_to_process = folders_to_process[1:]
            wksp_ls, wksp_ls_out = self._wksp_folder_listing(path_to_process,
                                                        extended=True,
                                                        absolute_paths=True)

            if wksp_ls.returncode != 0:
                self.logger.error("Workspace listing error: %s", wksp_ls.stdout)

            for fp in wksp_ls_out:
                m_nb = re_notebook.match(fp)
                if m_nb is not None:
                    if self.match_filter(fp, pattern_filter):
                        files.append( ("NOTEBOOK",fp, m_nb.group(1).strip(), m_nb.group(2)))
                else:
                    m_dir = re_folder.match(fp)
                    if m_dir is not None:
                        if not omit_dirs:
                            files.append(("FOLDER", fp, m_dir.group(1).strip()+"/", ""))
                        if recursive:
                            folders_to_process.append(m_dir.group(1).strip())
                    elif allow_other and fp is not None and len(fp) > 0:
                        m_other = re_other.match(fp)
                        if self.match_filter(fp, pattern_filter):
                            files.append(("OTHER", fp, m_other.group(2).strip()+ " (L)" if m_other is not None else fp, ""))

        if showProgress:
            print(" ")

        # clean up output
        root = effective_path if effective_path.endswith("/") else effective_path+"/"
        if not absolute_paths:
            files = [ (x[0], x[1].replace(root, ""), x[2].replace(root, ""), x[3]) for x in files]

        return sorted(files, key=itemgetter(2))

    def adjust_local_paths(self, fpath,args):
        local_dir, local_patt = os.path.split(fpath)

        if local_dir.startswith(".."):
            raise ValueError("Paths with `..` not supported")

        if local_dir == '.' and args.recursive:
            local_dir="./**/"
            self.logger.debug("Replacing local_dir with [%s]", local_dir)

        local_path=os.path.join(local_dir, local_patt)

        if local_path.startswith("./") and args.absolute:
            local_path = os.path.join(os.getcwd(), local_path[2:])

        return local_path

    def diff_against_workspace(self, args):
        """Export command implementation"""
        self.logger.debug("starting diff")
        self.get_params(args)

        if "**" in args.src_path or "**" in args.wksp_path:
            args.recursive=True

        modified_files = self.get_modified_or_untracked_changes(args.src_path, recursive=args.recursive)

        if modified_files is not None and len(modified_files) > 0:
            self.logger.warning("There are modified or untracked files not checked in to local repo:\n  %s",
                                str(modified_files))

        local_path = self.adjust_local_paths(args.src_path, args)

        dir_contents = self.get_dir_listing(local_path, recursive=args.recursive)

        showProgress = not args.verbose and not args.debug
        wksp_contents = self.get_workspace_listing(args.wksp_path, extended=args.long,
                                                   absolute_paths=args.absolute,
                                                   recursive=args.recursive,
                                                   showProgress=showProgress,
                                                   omit_dirs=True)

        display_contents=[ x[2] for x in wksp_contents]
        print("local file system contents:", dir_contents)
        print("workspace contents:", display_contents)

    def import_to_workspace(self, args):
        """Export command implementation"""
        self.logger.debug("starting import")
        self.get_params(args)

        #  get list of files matching pattern

        if "**" in args.src_path or "**" in args.wksp_path:
            args.recursive=True

        args.absolute=False

        self.logger.debug("checking for uncommitted changes ")

        # get `git status -s` output for current directory
        # filter files from both listings with `..` paths
        modified_files = self.get_modified_or_untracked_changes(args.src_path, recursive=args.recursive)
        self.logger.debug("Uncommited changes : %s", modified_files)

        # check if any files that would be imported to workspace have uncommitted or untracked changes
        # if so, exit with error , unless `--force` was specified
        if modified_files is not None and len(modified_files) > 0 and not args.force:
            self.logger.error("There are modified or untracked files not checked in to local repo:\n  %s",
                                str(modified_files))
            raise RuntimeError("There are untracked or uncommitted changes: {}".format(str(modified_files)))

        local_path = self.adjust_local_paths(args.src_path, args)

        dir_contents = self.get_dir_listing(local_path, recursive=args.recursive)
        print(dir_contents)

        effective_path = self.mk_workspace_path(args.wksp_path)

        import_files = list(map( lambda x: (x, self.mk_workspace_path(effective_path, x)), dir_contents))

        #  determine folders needed on target
        folders=set([ os.path.dirname(x2[1]) for x2 in import_files])
        self.logger.debug("folders to create: %s", folders)

        #  make folders using `databricks workspace mkdirs`
        for f in folders:
            mkdir_cmd = ['databricks', 'workspace', 'mkdirs', '--profile', self.profile_to_use, f]
            self.add_command(mkdir_cmd)

        #  for each of the  files generate command to import them to the workspace
        # i.e databricks workspace import --language SCALA --format DBC src_file tgt_destination

        if not args.keep_extensions:
            import_files2 = []
            for f in import_files:
                src_file = f[0]
                tgt_file, ext = os.path.splitext(f[1])
                import_files2.append( (src_file, tgt_file) )
            import_files =import_files2
        self.logger.debug("files to import to workspace (src, target): %s", import_files)

        for x in import_files:
            cmd = ['databricks', 'workspace', 'import', '--profile', self.profile_to_use,
                   '--format', args.format, '--language', args.language]
            if args.overwrite:
                cmd.append("--overwrite")

            cmd.append(x[0])
            cmd.append(x[1])
            self.add_command(cmd)

        self.execute_cmds_ex(args)

    def read_defaults(self):
        """ Read defaults from configuration file"""
        self.homedir = os.path.expanduser('~')
        filepath='{}/.databricks_sync/config.txt'.format(self.homedir)
        try:
            with open(filepath) as f:
                logging.info("reading defaults from {}".format(filepath))
                self.config_text=f.read()
                self.config=json.loads(self.config_text)
        except Exception as err:
            self.logger.debug("Warning: Could not read configuration file - '{}'".format(filepath))
            self.logger.debug(str(err))
            self.config= { "default_profile": "",
                 "default_root": "",
                 "default_language":"",
                 "default_format":""
                 }

    def get_input(self, prompt, config_key, default, choices):
        """ Prompt user for input using config_key to get a default"""
        retval=input(prompt)
        if retval is None or len(retval) == 0:
            retval=self.config[config_key]
        return retval

    def configure(self, args):
        """Configure command for subsequent use"""

        # read the existing defaults if any
        self.read_defaults()
        default_language="PYTHON"
        default_format="SOURCE"

        # get new defaults
        prompt1="Enter default profile [{}]: ".format(self.config.get("default_profile"))
        default_profile=input(prompt1)
        if default_profile is None or len(default_profile) == 0:
            default_profile=self.config["default_profile"]

        prompt2="Enter default root directory [{}]: ".format( self.config.get("default_root") )
        default_root=input(prompt2)
        if default_root is None or len(default_root) == 0:
            default_root=self.config.get("default_root")

        self.logger.debug("Existing defaults are : %s", str(self.config))
        self.config={ "default_profile": default_profile,
                 "default_root": default_root,
                 "default_language": default_language,
                 "default_format": default_format
                 }
        self.logger.info("New defaults are : %s", str(self.config))

        self.homedir=os.path.expanduser('~')

        # make config directory if necessary
        config_dir = '{}/.databricks_sync'.format(self.homedir)
        try:
            os.mkdir(config_dir)
        except:
            self.logger.debug("Directory already exists: %s", config_dir)
            pass

        # write the config file
        config_file = '{}/config.txt'.format(config_dir)
        self.logger.info("Writing file : %s", config_file)

        with open(config_file, 'w') as f:
            f.write(json.dumps(self.config))
        self.logger.debug("Completed writing file : %s", config_file)

    def get_params(self, args):
        """ get the profile params"""
        self.read_defaults()

        if args.profile is not None:
            self.profile_to_use = args.profile
        else:
            self.profile_to_use = self.config['default_profile']
        self.logger.info("using profile: {}".format(self.profile_to_use))

    def mk_workspace_path(self, s, *argv):
        """ get path - add root path if not absolute"""
        path_root = ""
        if s.startswith("/"):
            path_root=s
        else:
            self.logger.debug("adding root path: {}".format(self.config['default_root']))
            path_root= os.path.join(self.config['default_root'], s)

        additional_paths = [x[2:] if x.startswith("./") else x for x in argv]
        if additional_paths is not None and len(additional_paths) > 0:
            return os.path.join(path_root , *additional_paths)
        else:
            return path_root


    def ls(self, args):
        """ workspace ls command """
        self.get_params(args)

        effective_path = self.mk_workspace_path(args.path)
        self.logger.info("listing contents of remote workspace [{}]:".format(effective_path))
        print("listing contents of remote workspace: {}".format(effective_path))


        showProgress = not args.verbose and not args.debug
        wksp_contents = self.get_workspace_listing(effective_path, extended=args.long,
                                                   absolute_paths=args.absolute,
                                                   recursive=args.recursive,
                                                   showProgress=showProgress,
                                                   allow_other=True)

        for x in wksp_contents:
            if args.long:
                print("  {}".format(x[1]))
            else:
                print("  {}".format(x[2]))


    def sync(self):
        """ Main entry point """
        parser, args = self.parse_args()

        if args.command == "ls":
            self.ls(args)
        elif args.command == "configure":
            self.configure(args)
        elif args.command == "export":
            self.export_from_workspace(args)
        elif args.command == "import":
            self.import_to_workspace(args)
        elif args.command == "diff":
            self.diff_against_workspace(args)
        else:
            parser.print_help()



if __name__=="__main__":
    DatabricksSync().sync()
