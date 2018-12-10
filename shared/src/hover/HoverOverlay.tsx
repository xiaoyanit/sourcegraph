import { HoverOverlayProps } from '@sourcegraph/codeintellify'
import { LOADING } from '@sourcegraph/codeintellify/lib/types'
import { LoadingSpinner } from '@sourcegraph/react-loading-spinner'
import H from 'history'
import { upperFirst } from 'lodash'
import { castArray } from 'lodash-es'
import AlertCircleOutlineIcon from 'mdi-react/AlertCircleOutlineIcon'
import CloseIcon from 'mdi-react/CloseIcon'
import InformationOutlineIcon from 'mdi-react/InformationOutlineIcon'
import * as React from 'react'
import { MarkupContent } from 'sourcegraph'
import { ActionItem, ActionItemProps } from '../actions/ActionItem'
import { HoverMerged } from '../api/client/types/hover'
import { ExtensionsControllerProps } from '../extensions/controller'
import { PlatformContextProps } from '../platform/context'
import { asError, isErrorLike } from '../util/errors'
import { FileSpec, RepoSpec, ResolvedRevSpec, RevSpec } from '../util/url'
import { highlightCodeSafe, renderMarkdown, toNativeEvent } from './helpers'

const transformMouseEvent = (handler: (event: MouseEvent) => void) => (event: React.MouseEvent<HTMLElement>) =>
    handler(toNativeEvent(event))

export type HoverContext = RepoSpec & RevSpec & FileSpec & ResolvedRevSpec

export type HoverData = HoverMerged

interface Props
    extends HoverOverlayProps<HoverContext, HoverData, ActionItemProps>,
        ExtensionsControllerProps,
        PlatformContextProps {
    location: H.Location
}

export const HoverOverlay: React.FunctionComponent<Props> = ({
    hoverOrError,
    hoverRef,
    onCloseButtonClick,
    overlayPosition,
    showCloseButton,
    actionsOrError,
    className = '',
    extensionsController,
    platformContext,
    location,
}) => (
    <div
        className={`hover-overlay card ${className}`}
        ref={hoverRef}
        // tslint:disable-next-line:jsx-ban-props needed for dynamic styling
        style={
            overlayPosition
                ? {
                      opacity: 1,
                      visibility: 'visible',
                      left: overlayPosition.left + 'px',
                      top: overlayPosition.top + 'px',
                  }
                : {
                      opacity: 0,
                      visibility: 'hidden',
                  }
        }
    >
        {showCloseButton && (
            <button
                className="hover-overlay__close-button btn btn-icon"
                onClick={onCloseButtonClick ? transformMouseEvent(onCloseButtonClick) : undefined}
            >
                <CloseIcon className="icon-inline" />
            </button>
        )}
        <div className="hover-overlay__contents">
            {hoverOrError === LOADING ? (
                <div className="hover-overlay__row hover-overlay__loader-row">
                    <LoadingSpinner className="icon-inline" />
                </div>
            ) : isErrorLike(hoverOrError) ? (
                <div className="hover-overlay__row hover-overlay__hover-error alert alert-danger">
                    <h4>
                        <AlertCircleOutlineIcon className="icon-inline" /> Error fetching hover from language server:
                    </h4>
                    {upperFirst(hoverOrError.message)}
                </div>
            ) : (
                // tslint:disable-next-line deprecation We want to handle the deprecated MarkedString
                hoverOrError &&
                castArray<string | MarkupContent | { language: string; value: string }>(hoverOrError.contents)
                    .map(value => (typeof value === 'string' ? { kind: 'markdown', value } : value))
                    .map((content, i) => {
                        if ('kind' in content || !('language' in content)) {
                            if (content.kind === 'markdown') {
                                try {
                                    return (
                                        <div
                                            className="hover-overlay__content hover-overlay__row e2e-tooltip-content"
                                            key={i}
                                            dangerouslySetInnerHTML={{ __html: renderMarkdown(content.value) }}
                                        />
                                    )
                                } catch (err) {
                                    return (
                                        <div className="hover-overlay__row alert alert-danger">
                                            <strong>
                                                <AlertCircleOutlineIcon className="icon-inline" /> Error rendering hover
                                                content
                                            </strong>{' '}
                                            {upperFirst(asError(err).message)}
                                        </div>
                                    )
                                }
                            }
                            return content.value
                        }
                        return (
                            <code
                                className="hover-overlay__content hover-overlay__row e2e-tooltip-content"
                                key={i}
                                dangerouslySetInnerHTML={{
                                    __html: highlightCodeSafe(content.value, content.language),
                                }}
                            />
                        )
                    })
            )}
        </div>
        {actionsOrError === null ? (
            <div className="alert alert-info hover-overlay__alert-below">
                <InformationOutlineIcon className="icon-inline" /> No definition found
            </div>
        ) : isErrorLike(actionsOrError) ? (
            <div className="alert alert-danger hover-overlay__alert-below">
                <strong>
                    <AlertCircleOutlineIcon className="icon-inline" /> Error finding definition:
                </strong>{' '}
                {upperFirst(actionsOrError.message)}
            </div>
        ) : (
            actionsOrError !== undefined &&
            actionsOrError !== LOADING &&
            actionsOrError.length > 0 && (
                <div className="hover-overlay__actions hover-overlay__row">
                    {actionsOrError.map((action, i) => (
                        <ActionItem
                            key={i}
                            className="btn btn-secondary hover-overlay__action e2e-tooltip-j2d"
                            {...action}
                            variant="actionItem"
                            disabledDuringExecution={true}
                            showLoadingSpinnerDuringExecution={true}
                            showInlineError={true}
                            extensionsController={extensionsController}
                            platformContext={platformContext}
                            location={location}
                        />
                    ))}
                </div>
            )
        )}
    </div>
)
