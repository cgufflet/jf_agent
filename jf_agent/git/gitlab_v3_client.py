import gitlab3
import logging
import requests
from jf_agent import agent_logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class MissingSourceProjectException(Exception):
    pass


def log_and_print_request_error(e, action='making request', log_as_exception=False):
    try:
        response_code = e.response_code
    except AttributeError:
        # if the request error is a retry error, we won't have the code
        response_code = ''

    error_name = type(e).__name__

    if log_as_exception:
        agent_logging.log_and_print_error_or_warning(
            logger, logging.ERROR, msg_args=[error_name, response_code, action, e], error_code=3131,
        )
    else:
        agent_logging.log_and_print_error_or_warning(
            logger, logging.WARNING, msg_args=[error_name, response_code, action], error_code=3141
        )


class GitLabClient_v3:
    """
    __init__(self, server_url, token=None, convert_dates=True, ssl_verify=None, ssl_cert=None)

    Initialize a GitLab connection and optionally supply auth token. 
    convert_dates can be set to False to disable automatic conversion of date strings to datetime objects. 
    ssl_verify and ssl_cert are passed to python-requests as the verify and cert arguments, respectively.
    """

    def __init__(self, server_url, token, convert_dates=True, ssl_verify=None, ssl_cert=None):
        kwargs = {'token': token, 'convert_dates': convert_dates}
        if ssl_cert is not None:
            kwargs['ssl_cert'] = ssl_cert
        if not ssl_verify:
            kwargs['ssl_verify'] = False
        self.server_url = server_url
        self.agent_args = kwargs
        self.client = gitlab3.GitLab(server_url, **kwargs)
        self.version = '3'

    @staticmethod
    def _get_diff_string(diffs):
        diffs = [diff_str for sublist in diffs for diff_str in sublist]
        return '\n'.join(diffs)

    def expand_merge_request_data(self, merge_request):
        """
        Modifies the merge_request object by obtaining and adding the following attributes:
            - 'approved_by'     [object]
            - 'note_list'       [object]
            - 'commit_list'     [object]
            - 'target_project'  object
            - 'target_project'  object
            - 'diff'            string
        """
        target_project = self.get_project(merge_request.target_project_id)
        merge_request.target_project = target_project

        # the source project will be the same if the request is made from the same project
        # however, if the merge request is from a fork the source will be different and we'll
        # need to fetch its details
        if target_project.id != merge_request.source_project_id:
            try:
                merge_request.source_project = self.get_project(merge_request.source_project_id)
            except gitlab3.exceptions.GitLabException as e:
                if e.response_code == 404:
                    raise MissingSourceProjectException()
                raise
        else:
            merge_request.source_project = target_project

        try:
            merge_request.note_list = merge_request.notes()
        except (requests.exceptions.RetryError, gitlab3.exceptions.GitLabException) as e:
            log_and_print_request_error(
                e,
                f'fetching notes for merge_request {merge_request.id} -- '
                f'handling it as if it has no notes',
            )
            merge_request.note = []

        try:
            merge_request.diff = GitLabClient_v3._get_diff_string(merge_request.diff)
        except (requests.exceptions.RetryError, gitlab3.exceptions.GitLabException) as e:
            log_and_print_request_error(
                e,
                f'fetching changes for merge_request {merge_request.id} -- '
                f'handling it as if it has no diffs',
            )
            merge_request.diff = ''

        # convert the 'commit_list' generator into a list of objects
        merge_request.commit_list = self.get_mergerequest_commits(self, merge_request)
        return merge_request

    def get_event(self, checkout_sha=None, project_id=None, action_name='pushed to', find_all=True):
        sub_kwargs = None
        kwargs = {'action_name': action_name, 'find_all': find_all}
        proj = self.get_project(project_id=project_id)
        if checkout_sha is not None:
            sub_kwargs = {'checkout_sha': checkout_sha}
        events = proj.find_event(**kwargs)
        if sub_kwargs:
            for evnt in events:
                if evnt.data and evnt.action_name == 'pushed to':
                    if evnt.data['checkout_sha'] == sub_kwargs['checkout_sha']:
                        return evnt
        return events

    def get_group(self, group_id):
        return self.client.find_group(id=group_id)

    def get_project(self, project_id):
        return self.client.find_project(id=project_id)

    def list_group_projects(self, group_id=None):
        projects = self.client.projects()
        return projects

    def list_group_members(self, group_id):
        group = self.get_group(group_id)
        return group.members()

    def list_project_branches(self, project_id):
        project = self.get_project(project_id)
        return project.branches()

    def list_project_merge_requests(self, project_id, state_filter=None):
        project = self.get_project(project_id)
        mergerequests = project.merge_requests()
        if len(mergerequests) > 0:
            if state_filter:
                mergerequests = [entry for entry in mergerequests if entry.state in state_filter]
            mergerequests.sort(key=lambda x: x.created_at, reverse=True)
            mergerequests = self.add_v4_attrs('mergerequest', mergerequests, project)
            return mergerequests
        return mergerequests

    def list_project_commits(self, project_id, since_date):
        project = self.get_project(project_id)
        commits = list(project.commits())
        if len(commits) > 0:
            if since_date:
                commits = [
                    commit for commit in commits if commit.created_at > since_date
                ]  # datetime.strptime(since_date, "%m/%d/%Y").replace(tzinfo=timezone.utc)]
                commits.sort(key=lambda x: x.created_at, reverse=True)
            return commits
        return []

    def get_project_commit(self, project_id, sha):
        project = self.get_project(project_id)
        try:
            commit = project.find_commit(id=sha)
            return commit
        except gitlab3.exceptions.GitLabException:
            return None

    def get_mergerequest_commits(self, mergerequest, project=None):
        if project == None:
            project = self.client.find_project(id=mergerequest.project_id)
        commits = mergerequest.get_commits()
        if commits:
            for i, commit_dict in enumerate(commits):
                commit = project.find_commit(id=commit_dict['id'])
                if commit:
                    commits[i] = commit
            commits = [commit for commit in commits if type(commit) != type({})]
            return commits
        return commits

    def add_v4_attrs(self, type, dataset, project):
        if type == 'mergerequest':
            for i, entry in enumerate(dataset):
                mrg_evnt = self.get_event(
                    checkout_sha=entry.sha,
                    project_id=entry.project_id,
                    action_name='pushed to',
                    find_all=True,
                )
                mrg_date = mrg_evnt.created_at
                commits = self.get_mergerequest_commits(entry, project)
                entry.__setattr__('merge_date', mrg_date.isoformat())
                entry.__setattr__('approved_by', mrg_evnt.author['name'])
                entry.__setattr__('created_at', entry.created_at.isoformat())
                entry.__setattr__('updated_at', entry.updated_at.isoformat())
                entry.__setattr__('updated_at_dt', entry.updated_at)
                entry.__setattr__('closed_at', mrg_date.isoformat())
                entry.__setattr__('base_branch', entry.target_branch)
                entry.__setattr__('head_branch', entry.source_branch)
                if commits:
                    diffs = []
                    for commit in commits:
                        diffs.extend(commit.diff())
                    if len(diffs) > 0:
                        diffs = [obj['diff'].splitlines() for obj in diffs]
                    entry.__setattr__('diff', diffs)
                else:
                    entry.__setattr__('diff', None)
                dataset[i] = entry
        elif type == 'commit':

            print('commit reformatted')
        elif type == 'comment':
            print('commit reformatted')
        elif type == 'user':
            print('commit reformatted')
        return dataset
