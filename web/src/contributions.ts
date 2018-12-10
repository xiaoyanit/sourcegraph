import H from 'history'
import React from 'react'
import { Subscription } from 'rxjs'
import { ExtensionsControllerProps } from '../../shared/src/extensions/controller'
import { registerHoverContributions } from '../../shared/src/hover/actions'
import { PlatformContextProps } from '../../shared/src/platform/context'

interface Props extends ExtensionsControllerProps, PlatformContextProps {
    history: H.History
}

/**
 * A component that registers global contributions. It is implemented as a React component so that its
 * registrations use the React lifecycle.
 */
export class GlobalContributions extends React.Component<Props> {
    private subscriptions = new Subscription()

    public componentDidMount(): void {
        this.subscriptions.add(registerHoverContributions(this.props))
    }

    public componentWillUnmount(): void {
        this.subscriptions.unsubscribe()
    }

    public render(): JSX.Element | null {
        return null
    }
}
