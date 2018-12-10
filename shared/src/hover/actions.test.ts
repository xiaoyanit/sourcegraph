import { HoveredToken, LOADER_DELAY } from '@sourcegraph/codeintellify'
import { Location } from '@sourcegraph/extension-api-types'
import assert from 'assert'
import { createMemoryHistory } from 'history'
import { from, of } from 'rxjs'
import { first, map } from 'rxjs/operators'
import { TestScheduler } from 'rxjs/testing'
import { ActionItemProps } from '../actions/ActionItem'
import { EMPTY_MODEL } from '../api/client/model'
import { Services } from '../api/client/services'
import { CommandRegistry } from '../api/client/services/command'
import { ContributionRegistry } from '../api/client/services/contribution'
import { ProvideTextDocumentLocationSignature } from '../api/client/services/location'
import { ContributableMenu, ReferenceParams, TextDocumentPositionParams } from '../api/protocol'
import { getContributedActionItems } from '../contributions/contributions'
import { EMPTY_SETTINGS_CASCADE } from '../settings/settings'
import { toPrettyBlobURL } from '../util/url'
import { getDefinitionURL, getHoverActionsContext, HoverActionsContext, registerHoverContributions } from './actions'
import { HoverContext } from './HoverOverlay'

const FIXTURE_PARAMS: TextDocumentPositionParams = {
    textDocument: { uri: 'git://r?v#f' },
    position: { line: 1, character: 1 },
}

const FIXTURE_LOCATION: Location = {
    uri: 'git://r2?v2#f2',
    range: {
        start: { line: 2, character: 2 },
        end: { line: 3, character: 3 },
    },
}

const FIXTURE_HOVER_CONTEXT: HoveredToken & HoverContext = {
    repoName: 'r',
    commitID: 'v',
    rev: 'v',
    filePath: 'f',
    line: 2,
    character: 2,
}

// Use toPrettyBlobURL as the urlToFile passed to these functions because it results in the most readable/familiar
// expected test output.
const urlToFile = toPrettyBlobURL

const scheduler = () => new TestScheduler((a, b) => assert.deepStrictEqual(a, b))

describe('getHoverActionsContext', () => {
    it('shows a loader for the definition if slow', () =>
        scheduler().run(({ cold, expectObservable }) =>
            expectObservable(
                from(
                    getHoverActionsContext(
                        {
                            extensionsController: {
                                services: {
                                    textDocumentDefinition: {
                                        getLocations: () =>
                                            cold<Location[]>(`- ${LOADER_DELAY}ms --- d`, { d: [FIXTURE_LOCATION] }),
                                    },
                                    textDocumentReferences: {
                                        providersForDocument: () =>
                                            cold<ProvideTextDocumentLocationSignature<ReferenceParams, Location>[]>(
                                                'a',
                                                { a: [() => of(null)] }
                                            ),
                                    },
                                },
                            },
                            platformContext: { urlToFile },
                        },
                        FIXTURE_HOVER_CONTEXT
                    )
                )
            ).toBe(`a ${LOADER_DELAY - 1}ms (bc)d`, {
                a: {
                    'goToDefinition.showLoading': false,
                    'goToDefinition.url': null,
                    'goToDefinition.notFound': false,
                    'goToDefinition.error': false,
                    'findReferences.url': null,
                    hoverPosition: FIXTURE_PARAMS,
                },
                b: {
                    'goToDefinition.showLoading': true,
                    'goToDefinition.url': null,
                    'goToDefinition.notFound': false,
                    'goToDefinition.error': false,
                    'findReferences.url': null,
                    hoverPosition: FIXTURE_PARAMS,
                },
                c: {
                    'goToDefinition.showLoading': true,
                    'goToDefinition.url': null,
                    'goToDefinition.notFound': false,
                    'goToDefinition.error': false,
                    'findReferences.url': '/r@v/-/blob/f#L2:2&tab=references',
                    hoverPosition: FIXTURE_PARAMS,
                },
                d: {
                    'goToDefinition.showLoading': false,
                    'goToDefinition.url': '/r2@v2/-/blob/f2#L3:3',
                    'goToDefinition.notFound': false,
                    'goToDefinition.error': false,
                    'findReferences.url': '/r@v/-/blob/f#L2:2&tab=references',
                    hoverPosition: FIXTURE_PARAMS,
                },
            } as { [key: string]: HoverActionsContext })
        ))

    it('shows no loader for the definition if fast', () =>
        scheduler().run(({ cold, expectObservable }) =>
            expectObservable(
                from(
                    getHoverActionsContext(
                        {
                            extensionsController: {
                                services: {
                                    textDocumentDefinition: {
                                        getLocations: () => cold<Location[]>(`-b`, { b: [FIXTURE_LOCATION] }),
                                    },
                                    textDocumentReferences: {
                                        providersForDocument: () =>
                                            cold<ProvideTextDocumentLocationSignature<ReferenceParams, Location>[]>(
                                                'a',
                                                { a: [() => of(null)] }
                                            ),
                                    },
                                },
                            },
                            platformContext: { urlToFile },
                        },
                        FIXTURE_HOVER_CONTEXT
                    )
                )
            ).toBe(`a(bc)`, {
                a: {
                    'goToDefinition.showLoading': false,
                    'goToDefinition.url': null,
                    'goToDefinition.notFound': false,
                    'goToDefinition.error': false,
                    'findReferences.url': null,
                    hoverPosition: FIXTURE_PARAMS,
                },
                b: {
                    'goToDefinition.showLoading': false,
                    'goToDefinition.url': '/r2@v2/-/blob/f2#L3:3',
                    'goToDefinition.notFound': false,
                    'goToDefinition.error': false,
                    'findReferences.url': null,
                    hoverPosition: FIXTURE_PARAMS,
                },
                c: {
                    'goToDefinition.showLoading': false,
                    'goToDefinition.url': '/r2@v2/-/blob/f2#L3:3',
                    'goToDefinition.notFound': false,
                    'goToDefinition.error': false,
                    'findReferences.url': '/r@v/-/blob/f#L2:2&tab=references',
                    hoverPosition: FIXTURE_PARAMS,
                },
            } as { [key: string]: HoverActionsContext })
        ))
})

describe('getDefinitionURL', () => {
    it('emits null if the locations result is null', async () => {
        assert.strictEqual(
            await getDefinitionURL(
                { urlToFile },
                {
                    getLocations: () => of(null),
                },
                FIXTURE_PARAMS
            )
                .pipe(first())
                .toPromise(),
            null
        )
    })

    it('emits null if the locations result is empty', async () => {
        assert.strictEqual(
            await getDefinitionURL(
                { urlToFile },
                {
                    getLocations: () => of([]),
                },
                FIXTURE_PARAMS
            )
                .pipe(first())
                .toPromise(),
            null
        )
    })

    describe('if there is exactly 1 location result', () => {
        it('emits the definition URL with range', async () => {
            assert.deepStrictEqual(
                await getDefinitionURL(
                    { urlToFile },
                    {
                        getLocations: () => of<Location[]>([FIXTURE_LOCATION]),
                    },
                    FIXTURE_PARAMS
                )
                    .pipe(first())
                    .toPromise(),
                { url: '/r2@v2/-/blob/f2#L3:3', multiple: false }
            )
        })

        it('emits the definition URL without range', async () => {
            assert.deepStrictEqual(
                await getDefinitionURL(
                    { urlToFile },
                    {
                        getLocations: () => of<Location[]>([{ ...FIXTURE_LOCATION, range: undefined }]),
                    },
                    FIXTURE_PARAMS
                )
                    .pipe(first())
                    .toPromise(),
                { url: '/r2@v2/-/blob/f2', multiple: false }
            )
        })
    })

    it('emits the definition panel URL if there is more than 1 location result', async () => {
        assert.deepStrictEqual(
            await getDefinitionURL(
                { urlToFile },
                {
                    getLocations: () => of<Location[]>([FIXTURE_LOCATION, { ...FIXTURE_LOCATION, uri: 'other' }]),
                },
                FIXTURE_PARAMS
            )
                .pipe(first())
                .toPromise(),
            { url: '/r@v/-/blob/f#L2:2&tab=def', multiple: true }
        )
    })
})

describe('registerHoverContributions', () => {
    const contribution = new ContributionRegistry(of(EMPTY_MODEL), { data: of(EMPTY_SETTINGS_CASCADE) }, of({}))
    const commands = new CommandRegistry()
    const textDocumentDefinition: Pick<Services['textDocumentDefinition'], 'getLocations'> = {
        getLocations: () => of(null),
    }
    const history = createMemoryHistory()
    const subscription = registerHoverContributions({
        extensionsController: {
            services: {
                contribution,
                commands,
                textDocumentDefinition,
            },
        },
        platformContext: { urlToFile },
        history,
    })
    after(() => subscription.unsubscribe())

    const getHoverActions = (context: HoverActionsContext) =>
        contribution
            .getContributions(undefined, context)
            .pipe(
                first(),
                map(contributions => getContributedActionItems(contributions, ContributableMenu.Hover))
            )
            .toPromise()

    describe('getHoverActions', () => {
        const GO_TO_DEFINITION_ACTION: ActionItemProps = {
            action: {
                command: 'goToDefinition',
                commandArguments: ['{"textDocument":{"uri":"git://r?v#f"},"position":{"line":1,"character":1}}'],
                id: 'goToDefinition',
                title: 'Go to definition',
            },
            altAction: undefined,
        }
        const GO_TO_DEFINITION_PRELOADED_ACTION: ActionItemProps = {
            action: {
                command: 'open',
                commandArguments: ['/r2@v2/-/blob/f2#L3:3'],
                id: 'goToDefinition.preloaded',
                title: 'Go to definition',
            },
            altAction: undefined,
        }
        const FIND_REFERENCES_ACTION: ActionItemProps = {
            action: {
                command: 'open',
                commandArguments: ['/r@v/-/blob/f#L2:2&tab=references'],
                id: 'findReferences',
                title: 'Find references',
            },
            altAction: undefined,
        }

        it('shows goToDefinition (non-preloaded) when the definition is loading', async () =>
            assert.deepStrictEqual(
                await getHoverActions({
                    'goToDefinition.showLoading': true,
                    'goToDefinition.url': null,
                    'goToDefinition.notFound': false,
                    'goToDefinition.error': false,
                    'findReferences.url': null,
                    hoverPosition: FIXTURE_PARAMS,
                }),
                [GO_TO_DEFINITION_ACTION]
            ))

        it('shows goToDefinition (non-preloaded) when the definition had an error', async () =>
            assert.deepStrictEqual(
                await getHoverActions({
                    'goToDefinition.showLoading': false,
                    'goToDefinition.url': null,
                    'goToDefinition.notFound': false,
                    'goToDefinition.error': true,
                    'findReferences.url': null,
                    hoverPosition: FIXTURE_PARAMS,
                }),
                [GO_TO_DEFINITION_ACTION]
            ))

        it('hides goToDefinition when the definition was not found', async () =>
            assert.deepStrictEqual(
                await getHoverActions({
                    'goToDefinition.showLoading': false,
                    'goToDefinition.url': null,
                    'goToDefinition.notFound': true,
                    'goToDefinition.error': false,
                    'findReferences.url': null,
                    hoverPosition: FIXTURE_PARAMS,
                }),
                []
            ))

        it('shows goToDefinition.preloaded when goToDefinition.url is available', async () =>
            assert.deepStrictEqual(
                await getHoverActions({
                    'goToDefinition.showLoading': false,
                    'goToDefinition.url': '/r2@v2/-/blob/f2#L3:3',
                    'goToDefinition.notFound': false,
                    'goToDefinition.error': false,
                    'findReferences.url': null,
                    hoverPosition: FIXTURE_PARAMS,
                }),
                [GO_TO_DEFINITION_PRELOADED_ACTION]
            ))

        it('shows findReferences when the definition exists', async () =>
            assert.deepStrictEqual(
                await getHoverActions({
                    'goToDefinition.showLoading': false,
                    'goToDefinition.url': '/r2@v2/-/blob/f2#L3:3',
                    'goToDefinition.notFound': false,
                    'goToDefinition.error': false,
                    'findReferences.url': '/r@v/-/blob/f#L2:2&tab=references',
                    hoverPosition: FIXTURE_PARAMS,
                }),
                [GO_TO_DEFINITION_PRELOADED_ACTION, FIND_REFERENCES_ACTION]
            ))

        it('hides findReferences when the definition might exist (and is still loading)', async () =>
            assert.deepStrictEqual(
                await getHoverActions({
                    'goToDefinition.showLoading': true,
                    'goToDefinition.url': null,
                    'goToDefinition.notFound': false,
                    'goToDefinition.error': false,
                    'findReferences.url': '/r@v/-/blob/f#L2:2&tab=references',
                    hoverPosition: FIXTURE_PARAMS,
                }),
                [GO_TO_DEFINITION_ACTION, FIND_REFERENCES_ACTION]
            ))

        it('shows findReferences when the definition had an error', async () =>
            assert.deepStrictEqual(
                await getHoverActions({
                    'goToDefinition.showLoading': false,
                    'goToDefinition.url': null,
                    'goToDefinition.notFound': false,
                    'goToDefinition.error': true,
                    'findReferences.url': '/r@v/-/blob/f#L2:2&tab=references',
                    hoverPosition: FIXTURE_PARAMS,
                }),
                [GO_TO_DEFINITION_ACTION, FIND_REFERENCES_ACTION]
            ))

        it('shows findReferences when the definition was not found', async () =>
            assert.deepStrictEqual(
                await getHoverActions({
                    'goToDefinition.showLoading': false,
                    'goToDefinition.url': null,
                    'goToDefinition.notFound': true,
                    'goToDefinition.error': false,
                    'findReferences.url': '/r@v/-/blob/f#L2:2&tab=references',
                    hoverPosition: FIXTURE_PARAMS,
                }),
                [FIND_REFERENCES_ACTION]
            ))
    })

    describe('goToDefinition command', () => {
        it('reports no definition found', async () => {
            textDocumentDefinition.getLocations = () => of(null) // mock
            return assert.rejects(
                () =>
                    commands.executeCommand({ command: 'goToDefinition', arguments: [JSON.stringify(FIXTURE_PARAMS)] }),
                { message: 'No definition found.' }
            )
        })

        it('reports panel already visible', async () => {
            textDocumentDefinition.getLocations = () =>
                of([FIXTURE_LOCATION, { ...FIXTURE_LOCATION, uri: 'git://r3?v3#f3' }]) // mock
            history.push('/r@v/-/blob/f#L2:2&tab=def')
            return assert.rejects(
                () =>
                    commands.executeCommand({ command: 'goToDefinition', arguments: [JSON.stringify(FIXTURE_PARAMS)] }),
                { message: 'Multiple definitions shown in panel below.' }
            )
        })

        it('reports already at the definition', async () => {
            textDocumentDefinition.getLocations = () => of([FIXTURE_LOCATION]) // mock
            history.push('/r2@v2/-/blob/f2#L3:3')
            return assert.rejects(
                () =>
                    commands.executeCommand({ command: 'goToDefinition', arguments: [JSON.stringify(FIXTURE_PARAMS)] }),
                { message: 'Already at the definition.' }
            )
        })
    })
})
